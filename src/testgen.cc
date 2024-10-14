
/*
   testgen -- Generate test data for gdbm-rs
  
   Build with:
   $ g++ -Wall -O2 -o testgen testgen.cc -lgdbm

 */

#include <string>
#include <vector>
#include <stdio.h>
#include <unistd.h>
#include <cassert>
#include <time.h>
#include <ctype.h>
#include <gdbm.h>

using namespace std;

static const unsigned int N_REC = 10001;

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
		data.size());

	bool first = true;
	for (auto & p : data) {
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
"\t\t\tAvailable plans: basic, empty\n"
"\t-n Make DB numsync\n"
	);
}

int main (int argc, char *argv[])
{
	int opt;
	char *out_fn = NULL;
	char *out_json = NULL;
	string plan = "basic";
  bool numsync = false;

	while ((opt = getopt(argc, argv, "o:j:p:n")) != -1) {
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

	fprintf(stderr, "Unknown test plan %s\n", plan.c_str());
	return 1;
}
