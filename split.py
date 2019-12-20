def merge_n_label(directory):
    filenames = [str(year) for year in range(2010, 2020)]
    target_file = open("allyears.tsv", "w+")
    total_urls = 0

    for filename in filenames:
        print("THE YEAR IS %s" % filename)
        path = "%s/%s.tsv" % (directory, filename)
        with open(path) as f:
            for ctr, line in enumerate(f):
                target_file.write("%s\t%s\r\n" % (line.strip(), filename))
                total_urls += 1

    print("There are %i URLs total!" % total_urls)
    target_file.close()