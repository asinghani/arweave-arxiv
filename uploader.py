import boto3, os, glob, json, sys
from time import sleep
from datetime import datetime, time
from arweave import Transaction, Wallet
import logging

import time

from webhook import report_upload, report_uploadtoobig

millis = lambda: int(round(time.time() * 1000))

if len(sys.argv) < 3:
    print("Usage: {} <wallet filename> <DEV/STAGING/PROD>".format(sys.argv[0]))

wallet = Wallet(sys.argv[1])

DEBUG = True

logging.basicConfig(level=logging.ERROR)

# Determine total # of blocks
NUM_BLOCKS = 0
with open("blocks.csv") as f:
    for line in f:
        if len(line) > 5:
            NUM_BLOCKS += 1

def printd(x):
    if DEBUG:
        print("[{}] {}".format(str(datetime.now()), x))


def log_toobig(arxivID, year, month, block, pdf_name):
    with open("too_big.csv", "a") as f:
        f.write("{},{},{},{},{}\n".format(arxivID, year, month, block, pdf_name))

    report_uploadtoobig(arxivID, block)

def upload(arxivID, year, month, block, pdf_name):
    version = 1
    source = "ARXIV"

    filename = "out/{}/{}".format(block, pdf_name)

    # Check the file size
    file_size = os.stat(filename).st_size

    if file_size > 9000000: # 9MB
        printd("Paper {} in block {} too big to upload".format(arxivID, block))
        log_toobig(arxivID, year, month, block, pdf_name)
        return None

    with open(filename, "rb") as pdf_file:
        txn = Transaction(wallet, data=pdf_file.read())

    txn.add_tag("App-Name", "paper-archiver")
    txn.add_tag("App-Env", sys.argv[2])
    txn.add_tag("App-Version", str(version))
    txn.add_tag("Source", source)
    txn.add_tag("Id", arxivID)
    txn.add_tag("Year", str(year))
    txn.add_tag("Month", str(month))
    txn.add_tag("Block", block)
    txn.add_tag("Content-Type", "application/pdf")

    txn.sign()
    txn.send()

    txid = txn.id


    printd("Uploaded {}, txid={}".format(arxivID, txid))
    sleep(120)

    return txid

def mark_block(_id, completed):
    # Mark block as started if completed = False, else mark as completed
    csv = ""

    with open("blocks.csv") as f:
        for line in f:
            if len(line) < 5:
                continue

            if line.split(",")[0] == _id:
                line = line.strip()
                line = line[:-2]
                line += ",{}\n".format("3" if completed else "2")

            csv += line

    with open("blocks.csv", "w+") as f:
        f.write(csv)

def mark_paper(block_id, paper_id, toobig, txid=""):
    csv = ""
    filename = "csv/{}.csv".format(block_id)

    with open(filename) as f:
        for line in f:
            if len(line) < 5:
                continue

            if line.split(",")[0] == paper_id:
                line = line.strip()
                line = line[:-4]
                line += ",{},{}\n".format("2" if toobig else "1", "0" if toobig else txid)

            csv += line

    with open(filename, "w+") as f:
        f.write(csv)

def get_seconds():
    return datetime.now().time().second

def get_block():
    printd("Reading blocks file")

    undownloaded = False

    with open("blocks.csv") as f:
        n = 0
        for line in f:
            line = line.strip()

            if len(line) < 5:
                continue
            n += 1

            state = int(line.split(",")[-1])
            if state == 1 or state == 2:
                return line, n

            if state == 0:
                undownloaded = True

    if not undownloaded:
        printd("All blocks done!")
    else:
        printd("Waiting for download")

    return None, 0

def get_paper(block_id):

    filename = "csv/{}.csv".format(block_id)

    with open(filename) as f:
        for line in f:
            if len(line) < 5:
                continue

            if line.split(",")[1] == "0":
                return line.split(",")[0]

    printd("Block {} done".format(block_id))
    return None

if __name__ == "__main":
    while True:
        while get_seconds() < 27 or get_seconds() > 33:
            sleep(1)

        next_block, n = get_block()

        if next_block is None:
            sleep(10)
            continue

        block_id, year, month, blockNum, filename, status = next_block.split(",")

        printd("Starting block {}".format(block_id))

        mark_block(block_id, completed=False)

        while True:
            paper_id = get_paper(block_id)

            if paper_id is None:
                break

            txid = upload(paper_id, year, month, block_id, paper_id+".pdf")

            if txid is None:
                mark_paper(block_id, paper_id, True)
            else:
                mark_paper(block_id, paper_id, False, txid)

            os.system("cd out; cd {}; rm {}; cd ..; cd ..;".format(block_id, paper_id+".pdf"))

        while get_seconds() > 55 or get_seconds() < 5:
            sleep(1)

        mark_block(block_id, completed=True)
        report_upload(block_id, n, NUM_BLOCKS)

