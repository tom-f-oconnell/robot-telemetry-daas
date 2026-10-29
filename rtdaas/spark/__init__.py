"""Spark jobs (thin orchestration around the pure-Python core).

Nothing in this package imports ``pyspark`` at module import time except the job
entry points themselves; shared helpers import it lazily so that ``ddl.py`` and
``decode.py`` can be used (and unit-tested) without a Spark installation.
"""
