s3cmd --requester-pays ls s3://arxiv/pdf/ > filenames.txt
python generate_csv.py

