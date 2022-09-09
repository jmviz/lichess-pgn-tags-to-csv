use crate::tags::{Tags, CHESS960, ECO, RESULTS, TERMINATIONS, TITLES};

use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

use anyhow::{anyhow, Ok, Result};
use bzip2::{read::MultiBzDecoder, write::BzEncoder, Compression};
use pgn_reader::{BufferedReader, RawHeader, Skip, Visitor};
use phf::Map;

enum Full {}
enum Minified {}
trait Format {}
impl Format for Full {}
impl Format for Minified {}

struct CsvWriter<Writer, Formatter> {
    writer: Writer,
    formatter: PhantomData<Formatter>,
    tags: Arc<Tags>,
    chess960: bool,
    prev: usize,
}

impl CsvWriter<BufWriter<File>, Full> {
    fn new(path: &Path, tags: Arc<Tags>, chess960: bool) -> Result<Self> {
        let writer = BufWriter::new(File::create(path)?);
        Ok(Self {
            writer,
            formatter: PhantomData,
            tags,
            chess960,
            prev: 0,
        })
    }
}

impl CsvWriter<BzEncoder<BufWriter<File>>, Full> {
    fn new_compress(path: &Path, tags: Arc<Tags>, chess960: bool) -> Result<Self> {
        let writer = BzEncoder::new(BufWriter::new(File::create(path)?), Compression::best());
        Ok(Self {
            writer,
            formatter: PhantomData,
            tags,
            chess960,
            prev: 0,
        })
    }
}

impl CsvWriter<BufWriter<File>, Minified> {
    fn new_minify(path: &Path, tags: Arc<Tags>, chess960: bool) -> Result<Self> {
        let writer = BufWriter::new(File::create(path)?);
        Ok(Self {
            writer,
            formatter: PhantomData,
            tags,
            chess960,
            prev: 0,
        })
    }
}

impl CsvWriter<BzEncoder<BufWriter<File>>, Minified> {
    fn new_compress_minify(path: &Path, tags: Arc<Tags>, chess960: bool) -> Result<Self> {
        let writer = BzEncoder::new(BufWriter::new(File::create(path)?), Compression::best());
        Ok(Self {
            writer,
            formatter: PhantomData,
            tags,
            chess960,
            prev: 0,
        })
    }
}

impl<Writer: Write, Formatter: Format> CsvWriter<Writer, Formatter> {
    fn write_newline(&mut self) {
        self.writer.write_all(b"\n").expect("write newline");
    }
    fn write_comma(&mut self) {
        self.writer.write_all(b",").expect("write comma");
    }
    fn write_value(&mut self, key: &[u8], value: &[u8]) {
        self.writer.write_all(value).unwrap_or_else(|_| {
            panic!(
                "Failed writing tag {} with value {}.",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            )
        });
    }
    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

impl<Writer: Write> CsvWriter<Writer, Full> {
    fn write_header(&mut self) -> Result<()> {
        for col in 0..self.tags.len() {
            let name = self.tags.index(col);
            self.write_tag(col, name, name);
        }
        self.write_newline();
        self.prev = 0;
        Ok(())
    }
    fn write_any_missing_tags(&mut self, i: usize) {
        for j in self.prev + 1..i {
            self.write_tag(j, self.tags.index(j), b"");
        }
    }
    fn write_tag(&mut self, i: usize, key: &[u8], value: &[u8]) {
        self.write_value(key, value);
        if i < self.tags.last_index() {
            self.write_comma();
        }
        self.prev = i;
    }
}

impl<Writer: Write> CsvWriter<Writer, Minified> {
    fn write_header(&mut self) -> Result<()> {
        for col in 0..self.tags.len() {
            let tag = self.tags.index(col);
            match tag {
                b"Event" => self.write_tag(col, b"HEADER", b"Tournament"),
                b"Site" => self.write_tag(col, b"HEADER", b"Game"),
                b"WhiteElo" => {
                    self.write_value(b"HEADER", b"WhiteRating");
                    self.write_comma();
                    self.write_value(b"HEADER", b"WhiteRatingProvisional");
                    if col < self.tags.last_index() {
                        self.write_comma();
                    }
                    self.prev += 1;
                }
                b"BlackElo" => {
                    self.write_value(b"HEADER", b"BlackRating");
                    self.write_comma();
                    self.write_value(b"HEADER", b"BlackRatingProvisional");
                    if col < self.tags.last_index() {
                        self.write_comma();
                    }
                    self.prev += 1;
                }
                b"TimeControl" => {
                    self.write_value(b"HEADER", b"ClockInitialTime");
                    self.write_comma();
                    self.write_value(b"HEADER", b"ClockIncrement");
                    if col < self.tags.last_index() {
                        self.write_comma();
                    }
                    self.prev += 1;
                }
                b"FEN" => {
                    if self.chess960 {
                        self.write_tag(col, b"HEADER", b"StartingPosition");
                    } else {
                        self.write_tag(col, b"HEADER", tag);
                    }
                }
                _ => self.write_tag(col, b"HEADER", tag),
            }
        }
        self.write_newline();
        Ok(())
    }
    fn write_any_missing_tags(&mut self, i: usize) {
        for j in self.prev + 1..i {
            self.write_tag(j, self.tags.index(j), b"");
        }
    }
    fn write_coded_value(
        &mut self,
        code_map: &Map<&'static [u8], &'static [u8]>,
        key: &[u8],
        value: &[u8],
    ) {
        let code = code_map.get(value).map_or(&b""[..], |v| *v);
        self.write_value(key, code);
    }
    fn write_tag(&mut self, i: usize, key: &[u8], value: &[u8]) {
        match key {
            // If there is a tournament link at the end of Event, get the tournament id.
            // E.g. "Rated Chess960 tournament https://lichess.org/tournament/O5dkHvDT" -> "O5dkHvDT"
            // Similarly, for Site: "https://lichess.org/PpwPOZMq" -> "PpwPOZMq"
            b"Event" | b"Site" => {
                let value = value
                    .rsplit(|&v| v == b'/')
                    .next()
                    .and_then(|v| (v.len() < value.len()).then(|| v))
                    .unwrap_or(b"");
                self.write_value(key, value);
            }
            b"Result" => {
                self.write_coded_value(&RESULTS, key, value);
            }
            b"WhiteElo" | b"BlackElo" => {
                if let Some(b'?') = value.last() {
                    self.write_value(b"Rating", value.get(..value.len() - 1).unwrap_or(b""));
                    self.write_comma();
                    self.write_value(b"RatingProvisional", b"1");
                } else {
                    self.write_value(b"Rating", value);
                    self.write_comma();
                    self.write_value(b"RatingProvisional", b"0");
                }
            }
            b"WhiteRatingDiff" | b"BlackRatingDiff" => {
                if let Some(b'+') = value.first() {
                    self.write_value(b"RatingDiff", value.get(1..).unwrap_or(b""));
                } else {
                    self.write_value(b"RatingDiff", value);
                }
            }
            b"WhiteTitle" | b"BlackTitle" => {
                self.write_coded_value(&TITLES, key, value);
            }
            b"ECO" => {
                self.write_coded_value(&ECO, key, value);
            }
            b"TimeControl" => {
                let mut tc = value.split(|&v| v == b'+');
                let clock_initial_time = tc.next().unwrap_or(b"");
                if clock_initial_time == (b"-") {
                    // correspondence games just have TimeControl "-"
                    self.write_value(b"ClockInitialTime", b"");
                    self.write_comma();
                    self.write_value(b"ClockIncrement", b"");
                } else {
                    // TimeControl "300+0" -> ClockInitialTime "300", ClockIncrement "0"
                    let clock_increment = tc.next().unwrap_or(b"");
                    self.write_value(b"ClockInitialTime", clock_initial_time);
                    self.write_comma();
                    self.write_value(b"ClockIncrement", clock_increment);
                }
            }
            b"Termination" => {
                self.write_coded_value(&TERMINATIONS, key, value);
            }
            b"FEN" => {
                if self.chess960 {
                    // FEN "bbnrkqnr/pppppppp/8/8/8/8/PPPPPPPP/BBNRKQNR w KQkq - 0 1"
                    // -> StartingPosition "240"
                    let rank8 = value.split(|&v| v == b'/').next().unwrap_or(b"");
                    let sp = CHESS960.get(rank8).map_or(&b""[..], |v| *v);
                    self.write_value(b"StartingPosition", sp);
                } else {
                    self.write_value(key, value);
                }
            }
            _ => self.write_value(key, value),
        }

        if i < self.tags.last_index() {
            self.write_comma();
        }
        self.prev = i;
    }
}

impl<Writer: Write> Visitor for CsvWriter<Writer, Full> {
    type Result = ();

    fn header(&mut self, key: &[u8], value: RawHeader<'_>) {
        if let Some(i) = self.tags.get_index(key) {
            self.write_any_missing_tags(i);
            self.write_tag(i, key, value.as_bytes());
        }
    }
    fn end_headers(&mut self) -> Skip {
        Skip(true)
    }
    fn end_game(&mut self) {
        self.write_any_missing_tags(self.tags.len());
        self.write_newline();
        self.prev = 0;
    }
}

impl<Writer: Write> Visitor for CsvWriter<Writer, Minified> {
    type Result = ();

    fn header(&mut self, key: &[u8], value: RawHeader<'_>) {
        if let Some(i) = self.tags.get_index(key) {
            self.write_any_missing_tags(i);
            self.write_tag(i, key, value.as_bytes());
        }
    }
    fn end_headers(&mut self) -> Skip {
        Skip(true)
    }
    fn end_game(&mut self) {
        self.write_any_missing_tags(self.tags.len());
        self.write_newline();
        self.prev = 0;
    }
}

pub(crate) fn pgn2csv(
    pgn_path: &Path,
    csv_dir: &Path,
    tags: Arc<Tags>,
    minify: bool,
    compress: bool,
) -> Result<()> {
    let ext = if compress { "csv.bz2" } else { "csv" };
    // convert path/to/pgn/x.pgn.bz2 or path/to/pgn/x.pgn to path/to/pgn/x.ext
    let prefix = pgn_path.with_extension("").with_extension(ext);
    // convert path/to/pgn/x.ext to x.ext
    let csv_file_name = prefix
        .file_name()
        .ok_or_else(|| anyhow!("Empty pgn file name {}", pgn_path.display()))?;
    // convert x.ext to path/to/csv/x.ext
    let csv_path = csv_dir.join(csv_file_name);

    let chess960 = csv_file_name
        .to_str()
        .map_or(false, |f| f.contains("chess960"));

    if pgn_path
        .extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("bz2"))
    {
        let mut pgn = BufferedReader::new(MultiBzDecoder::new(File::open(&pgn_path)?));
        process_pgn(&mut pgn, &csv_path, tags, minify, compress, chess960)?;
    } else {
        let mut pgn = BufferedReader::new(File::open(&pgn_path)?);
        process_pgn(&mut pgn, &csv_path, tags, minify, compress, chess960)?;
    };

    Ok(())
}

fn process_pgn<R: Read>(
    pgn: &mut BufferedReader<R>,
    csv_path: &Path,
    tags: Arc<Tags>,
    minify: bool,
    compress: bool,
    chess960: bool,
) -> Result<()> {
    match (compress, minify) {
        (true, true) => {
            let mut csv_writer: CsvWriter<_, Minified> =
                CsvWriter::new_compress_minify(csv_path, tags, chess960)?;
            csv_writer.write_header()?;
            pgn.read_all(&mut csv_writer)?;
            csv_writer.flush()?;
        }
        (true, false) => {
            let mut csv_writer: CsvWriter<_, Full> =
                CsvWriter::new_compress(csv_path, tags, chess960)?;
            csv_writer.write_header()?;
            pgn.read_all(&mut csv_writer)?;
            csv_writer.flush()?;
        }
        (false, true) => {
            let mut csv_writer: CsvWriter<_, Minified> =
                CsvWriter::new_minify(csv_path, tags, chess960)?;
            csv_writer.write_header()?;
            pgn.read_all(&mut csv_writer)?;
            csv_writer.flush()?;
        }
        (false, false) => {
            let mut csv_writer: CsvWriter<_, Full> = CsvWriter::new(csv_path, tags, chess960)?;
            csv_writer.write_header()?;
            pgn.read_all(&mut csv_writer)?;
            csv_writer.flush()?;
        }
    }

    Ok(())
}
