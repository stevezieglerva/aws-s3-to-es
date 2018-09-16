cls

call ..\Scripts\activate
call python -m unittest integration_tests.py integration_tests_S3TextFromLambdaEvent.py

call deactivate
