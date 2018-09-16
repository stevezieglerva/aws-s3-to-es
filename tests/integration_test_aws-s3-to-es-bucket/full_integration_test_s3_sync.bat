REM Does full integration test by creating a new S3 file. Results can be checked in CloudWatch logs


set timestamp=%date:~10,4%-%date:~4,2%-%date:~7,2%-%time:~0,2%%time:~3,2%%time:~6,2%
REM Replace spaces with underscores
set timestamp=%timestamp: =_%


copy /y test_s3.txt ".\ping_tests\test_s3_%timestamp%.txt"
copy /y test_s3_b.txt ".\ping_tests\test_s3_b_%timestamp%.txt"
call aws s3 sync  . s3://aws-s3-to-es/ 


