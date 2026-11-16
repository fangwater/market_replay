/* Project children only. DuckDB sizes its import-time default thread pool via
 * libstdc++ hardware_concurrency -> glibc get_nprocs, ignoring affinity and
 * OMP/Rayon settings. Bound auto-sizing before import; explicit worker pools
 * keep their separately allocated budget. No system configuration is changed.
 */
int get_nprocs(void) { return 1; }
int get_nprocs_conf(void) { return 1; }
