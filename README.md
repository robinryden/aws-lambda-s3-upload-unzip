# aws-lambda-s3-upload-unzip

Lambdas written a long time ago, automation for fetching a lot of zipped files from sftp server, uploading to S3 bucket in aws. Unzipping etc.

sftp_s3_uploader: SFTP server -> upload all zipped files to S3 bucket.

s3_unzipper_upload: S3 bucket -> Unzip all files from bucket -> upload the unzipped files to new S3 folder structure.
