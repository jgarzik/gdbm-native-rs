extern crate gdbm_native;

use gdbm_native::{Alignment, Endian, Offset, OpenOptions};

#[test]
fn api_compact() {
    let test = |alignment, endian, offset, numsync| {
        // create a temporary database
        let mut db = OpenOptions::new()
            .write()
            .create()
            .alignment(Some(alignment))
            .offset(Some(offset))
            .endian(Some(endian))
            .numsync(numsync)
            .tempfile()
            .map_err(|e| format!("creating: {e}"))?;

        // add 0..1000
        (0usize..1000)
            .try_for_each(|n| {
                let v = vec![1u8; n];
                db.insert(&n, &v).map(|_| ())
            })
            .map_err(|e| format!("inserting entries: {e}"))?;

        // remove even numbere records to create "holes"
        (0usize..1000)
            .filter(|n| n % 2 == 0)
            .try_for_each(|n| db.remove(&n).map(|_| ()))
            .map_err(|e| format!("removing: {e}"))?;

        // compact
        db.compact().map_err(|e| format!("compacting: {e}"))?;

        // check database contents
        (0usize..1000)
            .try_for_each(|n| {
                let v = db
                    .get::<&usize, Vec<u8>>(&n)
                    .map_err(|e| format!("reading {n}: {e}"))?;
                if n % 2 != 0 {
                    (v == Some(vec![1; n]))
                        .then_some(())
                        .ok_or_else(|| format!("wrong read: {n}"))
                } else {
                    v.is_none()
                        .then_some(())
                        .ok_or_else(|| format!("unexpected read: {n}"))
                }
            })
            .map_err(|e| format!("removing: {e}"))?;

        // check database layout
        (alignment == db.alignment()
            && numsync == db.magic().is_numsync()
            && endian == db.magic().endian()
            && offset == db.magic().offset())
        .then_some(())
        .ok_or_else(|| "wrong layout".to_string())
    };

    [Alignment::Align64, Alignment::Align32]
        .into_iter()
        .try_for_each(|alignment| {
            [Endian::Little, Endian::Big]
                .into_iter()
                .try_for_each(|endian| {
                    [Offset::LFS, Offset::Small]
                        .into_iter()
                        .try_for_each(|offset| {
                            [false, true].into_iter().try_for_each(|numsync| {
                                test(alignment, endian, offset, numsync).map_err(|e| {
                                    format!(
                                    "{alignment:?} {offset:?} {endian:?} numsync {numsync}: {e}"
                                )
                                })
                            })
                        })
                })
        })
        .unwrap_or_else(|e| panic!("{e}"));
}
