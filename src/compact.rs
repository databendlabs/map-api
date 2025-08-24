// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compact operations on multi levels data.

use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::util;
use crate::MapApiRO;
use crate::MapKey;
use crate::SeqMarked;

/// Return the newer if it is a `not-found` record.
pub fn compact_seq_marked_pair<T>(newer: SeqMarked<T>, older: SeqMarked<T>) -> SeqMarked<T> {
    if newer.is_not_found() {
        older
    } else {
        newer
    }
}

/// Get a key from multi levels data.
///
/// Returns the first non-tombstone entry.
///
/// `persisted` is a series of persisted on disk levels.
///
/// - `K`: key type used in a map.
/// - `L`: type of the several top levels
/// - `PL`: the bottom persistent level.
pub async fn compacted_get<K, L, PL>(
    key: &K,
    levels: impl IntoIterator<Item = L>,
    persisted: impl IntoIterator<Item = PL>,
) -> Result<crate::SeqMarkedOf<K>, io::Error>
where
    K: MapKey,
    L: MapApiRO<K>,
    PL: MapApiRO<K>,
{
    for lvl in levels {
        let got = lvl.get(key).await?;
        if !got.is_not_found() {
            return Ok(got);
        }
    }

    for p in persisted {
        let got = p.get(key).await?;
        if !got.is_not_found() {
            return Ok(got);
        }
    }

    Ok(SeqMarked::new_not_found())
}

/// Iterate over a range of entries by keys from multi levels.
///
/// The returned iterator contains at most one entry for each key.
/// There could be tombstone entries: [`SeqMarked::is_tombstone`].
///
/// - `K`: key type used in a map.
/// - `TOP` is the type of the top level.
/// - `L` is the type of immutable levels.
/// - `PL` is the type of the persisted level.
///
/// Because the top level is very likely to be a different type from the immutable levels, i.e., it is writable.
///
/// `persisted` is a series of persisted on disk levels that have different types.
pub async fn compacted_range<K, R, TOP, L, PL>(
    range: R,
    top: Option<&TOP>,
    levels: impl IntoIterator<Item = L>,
    persisted: impl IntoIterator<Item = PL>,
) -> Result<crate::KVResultStream<K>, io::Error>
where
    K: MapKey,
    R: RangeBounds<K> + Clone + Send + Sync + 'static,
    TOP: MapApiRO<K> + 'static,
    L: MapApiRO<K>,
    PL: MapApiRO<K>,
{
    let mut kmerge = KMerge::by(util::by_key_seq);

    if let Some(t) = top {
        let strm = t.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    for lvl in levels {
        let strm = lvl.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    for p in persisted {
        let strm = p.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    // Merge entries with the same key, keep the one with larger internal-seq
    let coalesce = kmerge.coalesce(util::merge_kv_results);

    Ok(coalesce.boxed())
}

#[cfg(test)]
mod tests {

    use futures_util::TryStreamExt;

    use super::*;
    use crate::compact::compacted_get;
    use crate::compact::compacted_range;
    use crate::impls::immutable::Immutable;
    use crate::impls::level::Level;
    use crate::MapApi;
    use crate::SeqMarked;

    #[test]
    fn test_compact_seq_marked_pair() {
        assert_eq!(
            compact_seq_marked_pair(SeqMarked::new_normal(1, "a"), SeqMarked::new_normal(2, "b")),
            SeqMarked::new_normal(1, "a")
        );
        assert_eq!(
            compact_seq_marked_pair(SeqMarked::new_normal(1, "a"), SeqMarked::new_tombstone(2)),
            SeqMarked::new_normal(1, "a")
        );
        assert_eq!(
            compact_seq_marked_pair(SeqMarked::new_tombstone(1), SeqMarked::new_normal(2, "b")),
            SeqMarked::new_tombstone(1)
        );
        assert_eq!(
            compact_seq_marked_pair(
                SeqMarked::<()>::new_tombstone(0),
                SeqMarked::new_tombstone(2)
            ),
            SeqMarked::new_tombstone(2)
        );
    }

    #[tokio::test]
    async fn test_compacted_get() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;

        let l2 = l1.new_level();

        let got = compacted_get::<String, _, Level>(&s("a"), [&l0, &l1, &l2], []).await?;
        assert_eq!(got, SeqMarked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, Level>(&s("a"), [&l2, &l1, &l0], []).await?;
        assert_eq!(got, SeqMarked::new_tombstone(1));

        let got = compacted_get::<String, _, Level>(&s("a"), [&l1, &l0], []).await?;
        assert_eq!(got, SeqMarked::new_tombstone(1));

        let got = compacted_get::<String, _, Level>(&s("a"), [&l2, &l0], []).await?;
        assert_eq!(got, SeqMarked::new_normal(1, b("a")));
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_get_with_persisted_levels() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;

        let l2 = l1.new_level();

        let mut l3 = l2.new_level();
        l3.set(s("a"), Some(b("A"))).await?;

        let got = compacted_get::<String, _, Level>(&s("a"), [&l0, &l1, &l2], []).await?;
        assert_eq!(got, SeqMarked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, Level>(&s("a"), [&l2, &l1, &l0], []).await?;
        assert_eq!(got, SeqMarked::new_tombstone(1));

        let got = compacted_get::<String, _, &Level>(&s("a"), [&l2], [&l3]).await?;
        assert_eq!(got, SeqMarked::new_normal(2, b("A")));

        let got = compacted_get::<String, _, &Level>(&s("a"), [&l2], [&l2, &l3]).await?;
        assert_eq!(got, SeqMarked::new_normal(2, b("A")));
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range() -> anyhow::Result<()> {
        // ```
        // l2 |    b
        // l1 | a*    c*
        // l0 | a  b
        // ```
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;
        l0.set(s("b"), Some(b("b"))).await?;
        let l0 = Immutable::new_from_level(l0);

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;
        l1.set(s("c"), None).await?;
        let l1 = Immutable::new_from_level(l1);

        let mut l2 = l1.new_level();
        l2.set(s("b"), Some(b("b2"))).await?;

        // With top level
        {
            let got =
                compacted_range::<_, _, _, _, Level>(s("").., Some(&l2), [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), SeqMarked::new_tombstone(2)),
                (s("b"), SeqMarked::new_normal(3, b("b2"))),
                (s("c"), SeqMarked::new_tombstone(2)),
            ]);

            let got =
                compacted_range::<_, _, _, _, Level>(s("b").., Some(&l2), [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("b"), SeqMarked::new_normal(3, b("b2"))),
                (s("c"), SeqMarked::new_tombstone(2)),
            ]);
        }

        // Without top level
        {
            let got =
                compacted_range::<_, _, Level, _, Level>(s("").., None, [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), SeqMarked::new_tombstone(2)),
                (s("b"), SeqMarked::new_normal(2, b("b"))),
                (s("c"), SeqMarked::new_tombstone(2)),
            ]);

            let got =
                compacted_range::<_, _, Level, _, Level>(s("b").., None, [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("b"), SeqMarked::new_normal(2, b("b"))),
                (s("c"), SeqMarked::new_tombstone(2)),
            ]);
        }

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    #[test]
    fn test_compact_seq_marked_pair_edge_cases() {
        // Not found newer should return older
        assert_eq!(
            compact_seq_marked_pair(
                SeqMarked::<String>::new_not_found(),
                SeqMarked::new_normal(5, "older".to_string())
            ),
            SeqMarked::new_normal(5, "older".to_string())
        );

        // Not found newer with tombstone older
        assert_eq!(
            compact_seq_marked_pair(
                SeqMarked::<String>::new_not_found(),
                SeqMarked::new_tombstone(3)
            ),
            SeqMarked::new_tombstone(3)
        );

        // Not found both
        assert_eq!(
            compact_seq_marked_pair(
                SeqMarked::<String>::new_not_found(),
                SeqMarked::<String>::new_not_found()
            ),
            SeqMarked::<String>::new_not_found()
        );
    }

    #[tokio::test]
    async fn test_compacted_get_empty_levels() -> anyhow::Result<()> {
        // No levels at all
        let got = compacted_get::<String, Level, Level>(&s("missing"), [], []).await?;
        assert!(got.is_not_found());
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_get_key_not_found() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("b"), Some(b("b"))).await?;

        // Key not in any level
        let got = compacted_get::<String, _, Level>(&s("missing"), [&l0, &l1], []).await?;
        assert!(got.is_not_found());
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_get_only_tombstones() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?; // Tombstone

        let mut l2 = l1.new_level();
        l2.set(s("a"), None).await?; // Another tombstone

        // Should find first tombstone
        let got = compacted_get::<String, _, Level>(&s("a"), [&l2, &l1], []).await?;
        assert_eq!(got, SeqMarked::new_tombstone(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_empty_levels() -> anyhow::Result<()> {
        // No levels at all
        let got = compacted_range::<String, _, Level, &Level, Level>(s("").., None, [], []).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert!(got.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_single_level() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;
        l0.set(s("b"), Some(b("b"))).await?;

        let got = compacted_range::<_, _, Level, _, Level>(s("").., None, [&l0], []).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert_eq!(got, vec![
            (s("a"), SeqMarked::new_normal(1, b("a"))),
            (s("b"), SeqMarked::new_normal(2, b("b"))),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_with_persisted_only() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;
        l0.set(s("b"), Some(b("b"))).await?;

        // Only persisted levels, no in-memory levels
        let got =
            compacted_range::<String, _, Level, &Level, &Level>(s("").., None, [], [&l0]).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert_eq!(got, vec![
            (s("a"), SeqMarked::new_normal(1, b("a"))),
            (s("b"), SeqMarked::new_normal(2, b("b"))),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_overlapping_keys() -> anyhow::Result<()> {
        // Test that newer versions override older ones
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("old_a"))).await?;
        l0.set(s("c"), Some(b("c"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), Some(b("new_a"))).await?; // Override
        l1.set(s("b"), Some(b("b"))).await?; // New key

        let got = compacted_range::<_, _, Level, _, Level>(s("").., None, [&l1, &l0], []).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert_eq!(got, vec![
            (s("a"), SeqMarked::new_normal(3, b("new_a"))), /* Newer version (sequence continues from l0) */
            (s("b"), SeqMarked::new_normal(4, b("b"))),
            (s("c"), SeqMarked::new_normal(2, b("c"))),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_bounded_range() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;
        l0.set(s("b"), Some(b("b"))).await?;
        l0.set(s("c"), Some(b("c"))).await?;
        l0.set(s("d"), Some(b("d"))).await?;

        // Test bounded range
        let got =
            compacted_range::<_, _, Level, _, Level>(s("b")..=s("c"), None, [&l0], []).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert_eq!(got, vec![
            (s("b"), SeqMarked::new_normal(2, b("b"))),
            (s("c"), SeqMarked::new_normal(3, b("c"))),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range_all_tombstones() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some(b("a"))).await?;
        l0.set(s("b"), Some(b("b"))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?; // Tombstone
        l1.set(s("b"), None).await?; // Tombstone

        let got = compacted_range::<_, _, Level, _, Level>(s("").., None, [&l1], []).await?;
        let got = got.try_collect::<Vec<_>>().await?;
        assert_eq!(got, vec![
            (s("a"), SeqMarked::new_tombstone(2)), /* Both tombstones get same seq from the implementation */
            (s("b"), SeqMarked::new_tombstone(2)),
        ]);
        Ok(())
    }
}
