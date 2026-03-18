from bearplanes.data.wrds.crsp.downloader import download_crsp_dsf

# crspq.dsf_v2 -> daily stock data // COMPLETE
# crspq.wrds_dailyindexret -> indicies @TODO table does not exist error
# crspq.stkdistributions -> distributions // COMPLETE 
# crspq.stkdelists -> delistings // COMPLETE
def main():

    START_YEAR = 2010
    END_YEAR = 2026
    OUTPUT_DIR = "data/raw/wrds/crsp_delistings"
    TABLE_NAME = "crspq.stkdelists"

    download_crsp_dsf(START_YEAR, END_YEAR, OUTPUT_DIR, TABLE_NAME)



if __name__ == "__main__":
    main()