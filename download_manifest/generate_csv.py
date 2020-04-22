
# id (year/mo/block), year, month, block, filename, status={new(0), downloaded(1), started(2), done(3)}
with open("filenames.txt") as f:
        l = []

        for line in f:
            line = line.strip()

            if len(line) < 5:
                continue
            if "arXiv_pdf_manifest" in line:
                continue

            uri = line.split(" ")[-1]
            filename = uri.split("/")[-1]

            # TODO this code will stop working around 2090 because arxiv is stupid
            year = int(filename.split("_")[2][0:2])
            if year >= 90:
                year = 1900 + year
            else:
                year = 2000 + year

            month = int(filename.split("_")[2][2:4])
            block = int(filename.split("_")[3].split(".")[0])


            l.append((year, "{},{},{},{},{},0\n".format(filename.replace("arXiv_", "").split(".")[0],
                                                        year, month, block, filename)))


l = sorted(l, key=lambda a: a[0])

with open("../blocks.csv", "w+") as outfile:
    for year, line in l:
        outfile.write(line)
