
/*
   testgen -- Generate test data for gdbm-native

   Build with:
   $ g++ -Wall -O2 -o testgen testgen.cc -lgdbm

   See gen-platforms.sh, which runs it on several architectures.

 */

#include <set>
#include <string>
#include <vector>
#include <stddef.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>
#include <time.h>
#include <ctype.h>
#include <gdbm.h>

using namespace std;

static const unsigned int N_REC = 10001;
static const unsigned int N_REC_HASH = 1001;
static const unsigned int N_REC_AVAIL = 301;
static const unsigned int N_REC_MIXED = 400;

class kv_pair {
public:
	string key;
	string value;
};

static int gen_plan_empty(bool numsync, string db_fn, string json_fn)
{
	GDBM_FILE dbf = gdbm_open(db_fn.c_str(), 512, GDBM_NEWDB | (numsync ? GDBM_NUMSYNC : 0), 0666, NULL);
	if (!dbf) {
		fprintf(stderr, "gdbm_open failed\n");
		return 1;
	}

	gdbm_count_t count = 0;
	int rc = gdbm_count(dbf, &count);
	if (rc) {
		fprintf(stderr, "gdbm_count failed\n");
		return 1;
	}

	assert(count == 0);

	gdbm_close(dbf);

	FILE *f = fopen(json_fn.c_str(), "w");
	if (!f) {
		fprintf(stderr, "fopen failed\n");
		return 1;
	}

	time_t t = time(NULL);

	fprintf(f, "{"
	"  \"generated_by\":\"testgen\","
	"  \"generated_time\":\"%lu\","
	"  \"data_records\": %u,"
	"  \"data\": [",
		t,
		0);

	fprintf(f, "]}\n");
	fclose(f);

	return 0;
}

static int write_plan(const vector<kv_pair> & data, bool numsync, string db_fn, string json_fn,
		      const set<string> & removed = set<string>())
{
	GDBM_FILE dbf = gdbm_open(db_fn.c_str(), 512, GDBM_NEWDB | (numsync ? GDBM_NUMSYNC : 0), 0666, NULL);
	if (!dbf) {
		fprintf(stderr, "gdbm_open failed\n");
		return 1;
	}

	for (auto & p : data) {
		datum db_key = { (char *) p.key.c_str(), (int) p.key.size() };
		datum db_value = { (char *) p.value.c_str(), (int) p.value.size() };
		int rc = gdbm_store(dbf, db_key, db_value, GDBM_REPLACE);
		if (rc != 0) {
			fprintf(stderr, "gdbm_store failed, rc %d, key %s\n",
				rc, p.key.c_str());
			return 1;
		}
	}

	for (auto & key : removed) {
		datum db_key = { (char *) key.c_str(), (int) key.size() };
		if (gdbm_delete(dbf, db_key) != 0) {
			fprintf(stderr, "gdbm_delete failed, key %s\n", key.c_str());
			return 1;
		}
	}

	gdbm_close(dbf);

	FILE *f = fopen(json_fn.c_str(), "w");
	if (!f) {
		fprintf(stderr, "fopen failed\n");
		return 1;
	}

	time_t t = time(NULL);

	fprintf(f, "{"
	"  \"generated_by\":\"testgen\","
	"  \"generated_time\":\"%lu\","
	"  \"data_records\": %zu,"
	"  \"data\": [",
		t,
		data.size() - removed.size());

	bool first = true;
	for (auto & p : data) {
		if (removed.count(p.key))
			continue;

		fprintf(f, "%s[\"%s\",\"%s\"]",
			first ? "" : ",",
			p.key.c_str(),
			p.value.c_str());

		if (first)
			first = false;
	}

	fprintf(f, "]}\n");
	fclose(f);

	return 0;
}

static int gen_plan_basic(bool numsync, string db_fn, string json_fn)
{
	vector<kv_pair> data;

	char s[128];
	for (unsigned int i = 0; i < N_REC; i++) {
		kv_pair p;

		snprintf(s, sizeof(s), "key %u", i);
		p.key = s;

		snprintf(s, sizeof(s), "value %u", i);
		p.value = s;

		data.push_back(p);
	}

	return write_plan(data, numsync, db_fn, json_fn);
}

static int gen_plan_hash(bool numsync, string db_fn, string json_fn)
{
	vector<kv_pair> data;

	// keys contain bytes >= 0x80, whose hash depends on the signedness of char
	char s[128];
	for (unsigned int i = 0; i < N_REC_HASH; i++) {
		kv_pair p;

		snprintf(s, sizeof(s), "cl\xc3\xa9 %u \xe2\x82\xac", i);
		p.key = s;

		snprintf(s, sizeof(s), "valeur %u", i);
		p.value = s;

		data.push_back(p);
	}

	return write_plan(data, numsync, db_fn, json_fn);
}

static int gen_plan_avail(bool numsync, string db_fn, string json_fn)
{
	vector<kv_pair> data;
	set<string> removed;

	// Large values, mostly deleted: freed extents >= block size go to the header avail
	// table, overflowing it onto the avail stack.
	char s[128];
	for (unsigned int i = 0; i < N_REC_AVAIL; i++) {
		kv_pair p;

		snprintf(s, sizeof(s), "key %u", i);
		p.key = s;

		snprintf(s, sizeof(s), "value %u ", i);
		while (p.value.size() < 512 + (i * 397) % 1024)
			p.value += s;

		if (i % 3)
			removed.insert(p.key);

		data.push_back(p);
	}

	return write_plan(data, numsync, db_fn, json_fn, removed);
}

static string hex(const string & s)
{
	static const char digits[] = "0123456789abcdef";
	string out;
	for (unsigned char c : s) {
		out += digits[c >> 4];
		out += digits[c & 0xf];
	}
	return out;
}

// Keys of every kind (ASCII, non-ASCII UTF-8, binary with NUL and bytes
// >= 0x80), values from empty to several blocks long, then deletions so the
// free space spills onto the avail stack. Records are written to the JSON as
// hex, since they are not all text.
static int gen_plan_mixed(bool numsync, string db_fn, string json_fn, const char *dump_base)
{
	vector<kv_pair> data;
	set<string> removed;

	char s[128];
	for (unsigned int i = 0; i < N_REC_MIXED; i++) {
		kv_pair p;

		switch (i % 3) {
		case 0:
			snprintf(s, sizeof(s), "key %u", i);
			p.key = s;
			break;
		case 1:
			snprintf(s, sizeof(s), "cl\xc3\xa9 %u \xe2\x82\xac", i);
			p.key = s;
			break;
		default: {
			const char bin[] = { '\xff', '\x80', (char) (i & 0xff), (char) (i >> 8), '\0', 'x' };
			p.key = string(bin, sizeof(bin));
			break;
		}
		}

		if (i % 50) {
			snprintf(s, sizeof(s), "v%u:", i);
			while (p.value.size() < 1 + (i * 397) % 900)
				p.value += s;
			p.value.resize(1 + (i * 397) % 900);
		}

		if (i % 4 == 1)
			removed.insert(p.key);

		data.push_back(p);
	}

	GDBM_FILE dbf = gdbm_open(db_fn.c_str(), 512, GDBM_NEWDB | (numsync ? GDBM_NUMSYNC : 0), 0666, NULL);
	if (!dbf) {
		fprintf(stderr, "gdbm_open failed\n");
		return 1;
	}

	for (auto & p : data) {
		datum db_key = { (char *) p.key.data(), (int) p.key.size() };
		datum db_value = { (char *) p.value.data(), (int) p.value.size() };
		if (gdbm_store(dbf, db_key, db_value, GDBM_REPLACE) != 0) {
			fprintf(stderr, "gdbm_store failed: %s\n", gdbm_strerror(gdbm_errno));
			return 1;
		}
	}

	for (auto & key : removed) {
		datum db_key = { (char *) key.data(), (int) key.size() };
		if (gdbm_delete(dbf, db_key) != 0) {
			fprintf(stderr, "gdbm_delete failed: %s\n", gdbm_strerror(gdbm_errno));
			return 1;
		}
	}

	if (dump_base) {
		string bin = string(dump_base) + ".bin", txt = string(dump_base) + ".txt";
		if (gdbm_export(dbf, bin.c_str(), GDBM_NEWDB, 0644) < 0 ||
		    gdbm_dump(dbf, txt.c_str(), GDBM_DUMP_FMT_ASCII, GDBM_NEWDB, 0644) != 0) {
			fprintf(stderr, "dump failed: %s\n", gdbm_strerror(gdbm_errno));
			return 1;
		}
	}

	gdbm_close(dbf);

	FILE *f = fopen(json_fn.c_str(), "w");
	if (!f) {
		fprintf(stderr, "fopen failed\n");
		return 1;
	}

	fprintf(f, "{\"generated_by\":\"testgen\",\"records\":[");
	bool first = true;
	for (auto & p : data) {
		if (removed.count(p.key))
			continue;
		fprintf(f, "%s\n[\"%s\",\"%s\"]", first ? "" : ",",
			hex(p.key).c_str(), hex(p.value).c_str());
		first = false;
	}
	fprintf(f, "]}\n");
	fclose(f);

	return 0;
}

// The C type properties that make GDBM files and dumps platform-specific.
static int print_manifest(void)
{
	struct probe { int i; off_t o; };
	const unsigned int one = 1;

	printf("{\"gdbm_version\":\"%s\",\"char_signed\":%s,\"sizeof_long\":%zu,"
	       "\"sizeof_off_t\":%zu,\"alignof_off_t\":%zu,\"big_endian\":%s}\n",
	       gdbm_version, (char) -1 < 0 ? "true" : "false", sizeof(long),
	       sizeof(off_t), offsetof(struct probe, o),
	       *(const unsigned char *) &one ? "false" : "true");
	return 0;
}

static void usage(const char *progname)
{
	fprintf(stderr, "Usage: %s -o output-db -j output-json [options]\n", progname);
	fprintf(stderr,
"\n"
"Required Options:\n"
"\t-o DB-FILE\tOutput db\n"
"\t-j JSON-FILE\tOutput JSON metadata to file\n"
"Options:\n"
"\t-p PLAN\tGenerate according to test-plan PLAN\n"
"\t\t\tAvailable plans: basic, empty, hash, avail, mixed\n"
"\t-n Make DB numsync\n"
"\t-d BASE\tAlso write C dumps BASE.bin (binary) and BASE.txt (ASCII); mixed only\n"
"\t-m\tPrint platform manifest JSON and exit\n"
	);
}

int main (int argc, char *argv[])
{
	int opt;
	char *out_fn = NULL;
	char *out_json = NULL;
	char *dump_base = NULL;
	string plan = "basic";
	bool numsync = false;

	while ((opt = getopt(argc, argv, "o:j:p:nd:m")) != -1) {
		switch (opt) {
		case 'o':
			out_fn = optarg;
			break;
		case 'j':
			out_json = optarg;
			break;
		case 'p':
			plan = optarg;
			break;
		case 'n':
			numsync = true;
			break;
		case 'd':
			dump_base = optarg;
			break;
		case 'm':
			return print_manifest();
		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (!out_fn || !out_json) {
		usage(argv[0]);
		return 1;
	}

	if (plan == "basic")
		return gen_plan_basic(numsync, out_fn, out_json);
	if (plan == "empty")
		return gen_plan_empty(numsync, out_fn, out_json);
	if (plan == "hash")
		return gen_plan_hash(numsync, out_fn, out_json);
	if (plan == "avail")
		return gen_plan_avail(numsync, out_fn, out_json);
	if (plan == "mixed")
		return gen_plan_mixed(numsync, out_fn, out_json, dump_base);

	fprintf(stderr, "Unknown test plan %s\n", plan.c_str());
	return 1;
}
