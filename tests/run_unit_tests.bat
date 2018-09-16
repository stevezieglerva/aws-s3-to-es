cls

call ..\Scripts\activate
call python -m unittest unit_tests.py unit_tests_S3TextFromLambdaEvent.py

call deactivate
