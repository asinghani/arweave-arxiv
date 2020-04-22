import sys, os
from jose.utils import base64url_decode
from arweave import Wallet, Transaction

if len(sys.argv) < 3:
    print("Usage: {} <wallet> <transaction id> [output.pdf]".format(sys.argv[0]))
    sys.exit(1)


wallet = Wallet(sys.argv[1])
txn = Transaction(wallet, id=sys.argv[2])
txn.get_transaction()

tags_decoded = {base64url_decode(tag["name"].encode()).decode(): base64url_decode(tag["value"].encode()).decode() for tag in txn.tags}

if len(sys.argv) == 4:
    outfile = sys.argv[3]
elif "id" in tags_decoded:
    outfile = tags_decoded["id"] + ".pdf"
else:
    outfile = "output.pdf"

with open(outfile, "wb+") as f:
    f.write(base64url_decode(txn.data.encode()))

print("Info for transaction {}".format(txn.id))
print()
print("Tags:")
for tag in tags_decoded:
    print("{}: {}".format(tag, tags_decoded[tag]))
print()
print("Data (PDF format) stored in {}".format(outfile))
