/*
   cgdbm -- C GDBM side of the gdbm-native interoperability tests.
   See tests/interop/run.sh. The Rust side is examples/interop.rs; both
   implement the same commands:

     char-sign                      print "signed" or "unsigned" (C char)
     store DB HEXKEY VALUE          insert/replace one record
     fetch DB HEXKEY                print value, or NOTFOUND (exit 1)
     churn DB new|open SEED PHASES  random inserts/removes, then verify
     hold DB r|w SECS               open (locking) and hold, or report error
     export DB OUT | dump DB OUT    binary (gdbm_export) / ASCII (gdbm_dump) dump
     import DB IN | load DB IN      new DB from binary / ASCII dump; print
                                    "<count> <standard|numsync>"
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <gdbm.h>

static uint64_t rng;

/* same LCG as examples/interop.rs */
static uint64_t next(uint64_t n)
{
	rng = rng * 6364136223846793005ULL + 1442695040888963407ULL;
	return (rng >> 33) % n;
}

static datum unhex(const char *h)
{
	static char buf[256];
	datum d = { buf, (int) strlen(h) / 2 };
	for (int i = 0; i < d.dsize; i++)
		sscanf(h + 2 * i, "%2hhx", (unsigned char *) &buf[i]);
	return d;
}

static int fail(const char *what)
{
	printf("C %s: %s\n", what, gdbm_strerror(gdbm_errno));
	return 1;
}

/* avail lists, record count and every record must be readable */
static int verify(GDBM_FILE f)
{
	gdbm_count_t count = 0, n = 0;

	if (gdbm_avail_verify(f))
		return fail("avail_verify");
	if (gdbm_count(f, &count))
		return fail("count");
	for (datum k = gdbm_firstkey(f); k.dptr; n++) {
		datum v = gdbm_fetch(f, k);
		if (!v.dptr)
			return fail("fetch");
		free(v.dptr);
		datum nk = gdbm_nextkey(f, k);
		free(k.dptr);
		k = nk;
	}
	if (n != count) {
		printf("C iterated %llu of %llu records\n",
		       (unsigned long long) n, (unsigned long long) count);
		return 1;
	}
	return 0;
}

static int churn(const char *path, int create, int phases)
{
	static char buf[8192], key[32], pfx[32];
	int flags = create ? GDBM_NEWDB | GDBM_BSEXACT | GDBM_NUMSYNC : GDBM_WRITER;
	GDBM_FILE f = gdbm_open(path, 512, flags, 0644, NULL);

	if (!f)
		return fail("open");
	if (verify(f))
		return 1;

	for (int phase = 0; phase < phases; phase++) {
		uint64_t remove_pct = (phase % 2) ? 80 : 10;
		for (int op = 0; op < 500; op++) {
			uint64_t n = next(1000);
			snprintf(key, sizeof key, "%llu", (unsigned long long) n);
			datum k = { key, (int) strlen(key) };
			if (next(100) < remove_pct) {
				gdbm_delete(f, k);
				continue;
			}

			uint64_t len;
			switch (next(4)) {
			case 0: len = 1 + next(30); break;
			case 1: len = 1 + next(400); break;
			case 2: len = 400 + next(800); break;
			default: len = 1000 + next(3000); break;
			}
			int pl = snprintf(pfx, sizeof pfx, "%llu:", (unsigned long long) n);
			for (uint64_t i = 0; i < len; i++)
				buf[i] = pfx[i % pl];
			datum v = { buf, (int) len };
			if (gdbm_store(f, k, v, GDBM_REPLACE))
				return fail("store");
		}
	}

	if (verify(f))
		return 1;
	gdbm_close(f);
	return 0;
}

int main(int argc, char **argv)
{
	if (argc == 2 && !strcmp(argv[1], "char-sign")) {
		printf("%s\n", (char) -1 < 0 ? "signed" : "unsigned");
		return 0;
	}

	if (argc == 5 && !strcmp(argv[1], "store")) {
		GDBM_FILE f = gdbm_open(argv[2], 512, GDBM_WRCREAT, 0644, NULL);
		datum v = { argv[4], (int) strlen(argv[4]) };
		if (!f)
			return fail("open");
		if (gdbm_store(f, unhex(argv[3]), v, GDBM_REPLACE))
			return fail("store");
		gdbm_close(f);
		return 0;
	}

	if (argc == 4 && !strcmp(argv[1], "fetch")) {
		GDBM_FILE f = gdbm_open(argv[2], 0, GDBM_READER, 0, NULL);
		if (!f)
			return fail("open");
		datum v = gdbm_fetch(f, unhex(argv[3]));
		gdbm_close(f);
		if (!v.dptr) {
			printf("NOTFOUND\n");
			return 1;
		}
		printf("%.*s\n", v.dsize, v.dptr);
		return 0;
	}

	if (argc == 6 && !strcmp(argv[1], "churn")) {
		rng = strtoull(argv[4], NULL, 10);
		return churn(argv[2], !strcmp(argv[3], "new"), atoi(argv[5]));
	}

	if (argc == 4 && (!strcmp(argv[1], "export") || !strcmp(argv[1], "dump"))) {
		GDBM_FILE f = gdbm_open(argv[2], 0, GDBM_READER, 0, NULL);
		if (!f)
			return fail("open");
		int rc = !strcmp(argv[1], "export")
			? gdbm_export(f, argv[3], GDBM_NEWDB, 0644) < 0
			: gdbm_dump(f, argv[3], GDBM_DUMP_FMT_ASCII, GDBM_NEWDB, 0644);
		if (rc)
			return fail(argv[1]);
		gdbm_close(f);
		return 0;
	}

	if (argc == 4 && (!strcmp(argv[1], "import") || !strcmp(argv[1], "load"))) {
		GDBM_FILE f = gdbm_open(argv[2], 512, GDBM_NEWDB, 0644, NULL);
		unsigned long line = 0;
		gdbm_count_t count = 0;
		int format = 0;
		if (!f)
			return fail("open");
		int rc = !strcmp(argv[1], "import")
			? gdbm_import(f, argv[3], GDBM_REPLACE) < 0
			: gdbm_load(&f, argv[3], GDBM_REPLACE,
				    GDBM_META_MASK_MODE | GDBM_META_MASK_OWNER, &line);
		if (rc)
			return fail(argv[1]);
		if (gdbm_count(f, &count) || gdbm_setopt(f, GDBM_GETDBFORMAT, &format, sizeof format))
			return fail("count");
		printf("%llu %s\n", (unsigned long long) count, format ? "numsync" : "standard");
		gdbm_close(f);
		return 0;
	}

	if (argc == 5 && !strcmp(argv[1], "hold")) {
		int writer = argv[3][0] == 'w';
		GDBM_FILE f = gdbm_open(argv[2], 0, writer ? GDBM_WRITER : GDBM_READER, 0, NULL);
		if (!f) {
			if (gdbm_errno != GDBM_CANT_BE_READER && gdbm_errno != GDBM_CANT_BE_WRITER)
				return fail("open");
			printf("locked\n");
			return 0;
		}
		printf("ok\n");
		fflush(stdout);
		sleep(atoi(argv[4]));
		gdbm_close(f);
		return 0;
	}

	fprintf(stderr, "usage: see %s\n", __FILE__);
	return 2;
}
