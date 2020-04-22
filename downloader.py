import boto3, os, glob, json
from time import sleep
from datetime import datetime, time

from webhook import report_download

with open("/Users/anish/.nobkp/s3cred.json") as f:
    creds = json.load(f)

S3 = boto3.client("s3", aws_access_key_id=creds["accessKeyId"], aws_secret_access_key=creds["secretAccessKey"])

DEBUG = True

def printd(x):
    if DEBUG:
        print("[{}] {}".format(str(datetime.now()), x))

def download(_id, filename):
    #os.system("mkdir -p out/"+_id)

    with open("out/"+filename, "wb") as f:
        S3.download_fileobj("arxiv", "pdf/" + filename, f, ExtraArgs={"RequestPayer": "requester"})

    os.system("cd out; tar xf {}; mv {} {}; cd ..;".format(filename, filename.split("_")[2], _id))
    os.system("cd out; rm "+filename+"; cd ..;")

    paths = glob.glob("out/{}/*.pdf".format(_id))
    paths = [p for p in paths if ".pdf" in p]
    paths = [p.split("/")[-1].replace(".pdf", "") for p in paths]

    with open("csv/{}.csv".format(_id), "w+") as f:
        for p in paths:
            f.write("{},0,0\n".format(p))

def get_seconds():
    return datetime.now().time().second

CACHE_BLOCKS = 4

def get_work():
    printd("Reading blocks file")

    first_new = None
    existing = 0

    with open("blocks.csv") as f:
        for line in f:
            line = line.strip()

            if len(line) < 5:
                continue

            state = int(line.split(",")[-1])
            if state == 0:
                if first_new is None:
                    first_new = line
            elif state == 1 or state == 2:
                existing += 1

    if existing >= CACHE_BLOCKS:
        printd("Enough ({}) blocks already downloaded. Doing nothing.".format(existing))
    elif first_new is None:
        printd("All blocks finished. Doing nothing.")
    else:
        printd("Only {} blocks currently downloaded. Downloading new block {}.".format(
            existing, first_new.split(",")[0]))

        return first_new

    return None

def mark_as_completed(_id):
    csv = ""

    with open("blocks.csv") as f:
        for line in f:
            if len(line) < 5:
                continue

            if line.split(",")[0] == _id:
                # do things
                line = line.strip()
                line = line[:-2]
                line += ",1\n"

            csv += line

    with open("blocks.csv", "w+") as f:
        f.write(csv)


while True:
    while get_seconds() > 2 and get_seconds() < 58:
        sleep(1)

    next_block = get_work()

    if next_block is None:
        sleep(5)
        continue

    _id, year, month, blockNum, filename, status = next_block.split(",")

    download(_id, filename)
    printd("Downloaded {}".format(_id))
    report_download(_id, filename)

    while get_seconds() > 25 and get_seconds() < 35:
        sleep(1)

    mark_as_completed(_id)
    printd("Marked {} as completed".format(_id))


