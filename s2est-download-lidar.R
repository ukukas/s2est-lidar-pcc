librarian::shelf(magrittr, fs, raster, rgdal, readr, sf, plyr, dplyr, httr,
                 rlas, tidyr, parallel, doParallel)

read_template <- function(tile, res) {
  template <- base::sprintf('*T%s_*_AOT_%im.jp2', tile, res) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    raster::raster()
  return(template)
}

create_cunks <- function(tile, year, nchunk, frac) {
  epkids <- base::sprintf('*epk2T_tile%s_mets%i.csv', tile, year) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    readr::read_csv(file = ., col_names = TRUE, col_types = 'i--') %>%
    dplyr::arrange(id) %>%
    dplyr::pull(id) %>%
    base::split(x = ., f = base::ceiling(base::seq_along(.) / nchunk)) %>%
    sample_chunks(frac)
}

sample_chunks <- function(chunks, frac) {
  n <- base::ceiling(base::length(chunks) * frac)
  if(n < base::length(chunks)) {
    return(base::sample(chunks, n, replace = FALSE))
  }
  return(chunks)
}

download_laz <- function(epkid, year) {
  file <- fs::file_temp(ext = 'laz')
  httr::GET('https://geoportaal.maaamet.ee/index.php',
            httr::write_disk(file),
            query = base::list(
              lang_id = 1,
              plugin_act = 'otsing',
              kaardiruut = epkid,
              andmetyyp = 'lidar_laz_mets',
              dl = 1,
              f = base::sprintf('%i_%i_mets.laz', epkid, year),
              page_id = 614
            ))
  return(file)
}

process_chunk <- function(chunk, year, template, fldr, tile, res) {
  files <- base::sapply(chunk, function(x) download_laz(x, year))
  pts <- rlas::read.las(files, select = 'c',
                        filter = '-first_only -keep_class 2 5 6 9 17') %>%
    dplyr::select(X, Y, Classification) %>%
    sf::st_as_sf(coords = base::c('X', 'Y'), crs = 3301) %>%
    sf::st_transform(crs = template@crs)
  dplyr::bind_cols(sf::st_coordinates(pts),
                   classno = pts$Classification) %>%
    dplyr::mutate(P = raster::colFromX(template, X),
                  L = raster::rowFromY(template, Y)) %>%
    tidyr::drop_na() %>%
    dplyr::group_by(P, L) %>%
    dplyr::summarise(total = dplyr::n(),
                     flag = base::sum(base::as.integer(classno == 5)),
                     .groups = 'keep') %>%
    dplyr::ungroup() %>%
    readr::write_csv(base::sprintf('%s/grdtbl_%i_%s_%im_%s.csv',
                                   fldr, year, tile, res,
                                   base::paste0(chunk, collapse = '_')))
  fs::file_delete(files)
  base::rm(files, pts)
  base::gc()
  return(TRUE)
}

process_chunks <- function(cores, chunks, template, fldr, year, tile, res) {
  cl <- parallel::makePSOCKcluster(cores)
  doParallel::registerDoParallel(cl)
  plyr::llply(
    .data = chunks,
    .fun = process_chunk,
    template = template,
    fldr = fldr,
    year = year,
    tile = tile,
    res = res,
    .parallel = TRUE,
    .paropts = base::list(
      .packages = base::c('magrittr', 'fs', 'httr', 'rlas', 'dplyr',
                          'sf', 'raster', 'tidyr', 'readr'),
      .export = base::c('download_laz')
  )) %>% base::invisible()
  parallel::stopCluster(cl)
  return(TRUE)
}

main <- function() {
  cores <- parallel::detectCores() - 1
  tile <- '35VLF'
  res <- 10
  year <- 2018
  nchunk <- 1
  sample <- 0.1
  outdir <- base::sprintf('results_%i_%s_%im', year, tile, res)
  fs::dir_create(outdir)
  template <- read_template(tile, res)
  chunks <- create_cunks(tile, year, nchunk, sample)
  process_chunks(cores, chunks, template, outdir, year, tile, res) %>%
    base::invisible()
  return(TRUE)
}

main()
