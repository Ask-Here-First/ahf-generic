import os, sys, logging, unittest, importlib

try:
    # We have to import in the begining; otherwise static contents are not coveraged
    print("Load the Python coverage package ...")
    import coverage
    _cov = coverage.Coverage()
    _cov.erase()
    _cov.start()
    # Reload all loaded modules of name frid.* to cover all static context
    modules = [x for x in sys.modules.values() if x.__name__.startswith("frid.")]
    for module in modules:
        importlib.reload(module)
except ImportError:
    _cov = None

if _cov is not None:
    print("Running unit tests with coverage ...")
else:
    print("Running unit tests ...")

log_level = {
    'debug': logging.DEBUG, 'info': logging.INFO, 'error': logging.ERROR,
    'warn': logging.WARNING, 'warning': logging.WARNING,
}.get(os.getenv('FRID_LOG_LEVEL', 'warn').lower(), logging.INFO)
logging.basicConfig(level=log_level)

loader = unittest.TestLoader()
suite = loader.loadTestsFromNames(["frid.__test__", "frid.kvs.__test__", "frid.web.__test__"])
res = unittest.TextTestRunner(verbosity=(2 if log_level <= logging.INFO else 1)).run(suite)

for x, y in [("Skipped", res.skipped), ("Bungled", res.failures), ("Crashed", res.errors)]:
    if y:
        print(x)
        for a, b in y:
            print(f"   {a.__class__.__name__}: {b}")

if _cov is not None:
    _cov.stop()
    _cov.save()
    _cov.combine()
    print("Generating HTML converage report ...")
    _cov.html_report()
    print("Report is in [ htmlcov/index.html ].")
