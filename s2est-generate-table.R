librarian::shelf(magrittr, readr, dplyr)

read_pcc <- function(year, tile, res, minfrac) {
    base::sprintf('pcctbl_%i_%s_%im.csv', year, tile, res) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiidd') %>%
        dplyr::filter(total > res^2 * minfrac) %>%
        return()
}

read_bnd <- function(year, tile, res) {
    base::sprintf('bndtbl_%i_%s_%im.csv', year, tile, res) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiddddddddd') %>%
        return()
}

combine_tables <- function(pcctbl, bndtbl) {
    pcctbl %>%
        dplyr::inner_join(bndtbl, by = c('P', 'L')) %>%
        dplyr::select(cell, P, L, total, flag, pcc,
                      dplyr::any_of('lag'),
                      dplyr::starts_with('B')) %>%
        return()
}

calculate_indices <- function(df) {
    dplyr::mutate(df,
        DVI = B8A-B04,
        NDVI = (B8A-B04)/(B8A+B04),
        NDVIg = (B8A-B03)/(B8A+B03),
        CIg = (B8A/B03)-1,
        EVI = 2.5*((B8A-B04)/(B8A+6*B04-7.5*B02+1)),
        SAVI = 1.5*(B8A-B04)/(B8A+B04+0.5),
        NNIR = B8A/(B8A+B04+B03),
        WDRVI = (0.02*B8A-B04)/(0.02*B8A+B04)+(1-0.02)/(1+0.02),
        ARVI = (B8A-B04-(B02-B04))/(B8A+B04-(B02-B04)),
        VARIg = (B03-B04)/(B03+B04-B02),
        CIre = (B07/B05)-1,
        WDRVIre = (0.01*B07-B05)/(0.01*B07+B05)+(1-0.01)/(1+0.01),
        TCARI = 3*((B05-B04)-0.2*(B05-B03)*(B05/B04)),
        TCARI2 = 3*((B06-B05)-0.2*(B06-B03)*(B06/B05)),
        RSR = (B8A/B04)*((base::max(B11)-B11)/(base::max(B11)-base::min(B11))),
        ND = (B05-B07)/(B05+B07),
        SR = B05/B07,
        mND = (B02-B05)/(B02+B05-2*B07),
        mSR = (B02-B07)/(B05-B07),
        BSIT = (B05-B06-B07)/(B05+B06+B07),
        BSIV = (B02-B05)/(B07+B05),
        BSIW = (B03-B05+2*B07)/(B03+B05-2*B07)) %>%
        return()
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    res <- 20
    minfrac <- 0.1
    cdf <- read_pcc(year, tile, res, minfrac)
    sdf <- read_bnd(year, tile, res)
    combine_tables(cdf, sdf) %>%
        calculate_indices() %>%
        readr::write_csv(
            base::sprintf('regtbl_%i_%s_%im.csv', year, tile, res))
    return(TRUE)
}

main()

