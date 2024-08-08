.PHONY: all clean

all: run_all

run_all:
	@chmod +x scripts/run_all.sh
	@scripts/run_all.sh $(ARGS)

clean:
	@rm -f data/index_file.gz data/output_urls.txt
	@rm -f logs/spark_logs.txt logs/script_logs.txt
