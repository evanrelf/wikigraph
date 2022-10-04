use clap::Parser as _;
use quick_xml::events::Event;
use regex::Regex;
use std::{fs::File, io::BufReader, path::PathBuf, thread};

#[derive(clap::Parser)]
struct Args {
    /// Wikipedia dump file (multistream *.xml.bz2)
    input: PathBuf,

    /// SQLite database file
    #[arg(long)]
    database: PathBuf,
}

fn main() {
    let args = Args::parse();

    let (regex_tx, regex_rx) = flume::bounded(512);
    let (sqlite_tx, sqlite_rx) = flume::bounded(512);

    thread::scope(|s| {
        s.spawn(|| xml_thread(args.input, regex_tx));
        s.spawn(|| regex_thread(regex_rx, sqlite_tx));
        s.spawn(|| sqlite_thread(args.database, sqlite_rx));
    });
}

fn xml_thread(xml_file: PathBuf, regex_tx: flume::Sender<(String, String)>) {
    // // *.xml.bz2
    // let file =File::open(xml_bzip_file).unwrap();
    // let bzip2_decoder = bzip2::read::MultiBzDecoder::new(file);
    // let bufreader = BufReader::new(bzip2_decoder);
    // let mut xml_reader = quick_xml::Reader::from_reader(bufreader);

    // *.xml
    let mut xml_reader = quick_xml::Reader::from_file(xml_file).unwrap();

    let mut buffer = Vec::new();
    let mut in_title = false;
    let mut title = None;
    let mut in_text = false;

    loop {
        match xml_reader.read_event_into(&mut buffer) {
            Err(err) => eprintln!(
                "Error at position {}: {:?}",
                xml_reader.buffer_position(),
                err
            ),
            Ok(Event::Start(start)) if start.name().into_inner() == b"title" => in_title = true,
            Ok(Event::Start(start)) if start.name().into_inner() == b"text" => in_text = true,
            Ok(Event::Text(text)) if in_title => {
                title = Some(text.unescape().unwrap().into_owned());
            }
            Ok(Event::Text(text)) if in_text => match &title {
                Some(title) => {
                    let text = text.unescape().unwrap().into_owned();
                    regex_tx.send((title.clone(), text)).unwrap();
                }
                None => {
                    eprintln!(
                        "Error at position {}: In text without title",
                        xml_reader.buffer_position()
                    );
                }
            },
            Ok(Event::End(_)) => {
                in_title = false;
                in_text = false;
            }
            Ok(Event::Eof) => break,
            _ => (),
        }
        buffer.clear();
    }

    println!("[xml thread] Exiting");
}

fn regex_thread(
    regex_rx: flume::Receiver<(String, String)>,
    sqlite_tx: flume::Sender<(String, String)>,
) {
    let regex = Regex::new(r"(?:\[\[)([^\[\]]+?)(?:\|[^\[\]]*)?(?:\]\])").unwrap();

    while let Ok((title, text)) = regex_rx.recv() {
        for capture in regex.captures_iter(&text) {
            sqlite_tx
                .send((title.clone(), capture[1].to_string()))
                .unwrap();
        }
    }

    println!("[regex thread] Exiting");
}

fn sqlite_thread(database_file: PathBuf, sqlite_rx: flume::Receiver<(String, String)>) {
    let sqlite = rusqlite::Connection::open(database_file).unwrap();

    println!("[sqlite thread] Creating table");
    sqlite
        .execute_batch(
            "
            BEGIN;

            CREATE TABLE IF NOT EXISTS vertices (
                source TEXT NOT NULL,
                target TEXT NOT NULL
            ) STRICT;

            COMMIT;
            ",
        )
        .unwrap();

    let mut insert = sqlite
        .prepare("INSERT INTO vertices VALUES(?, ?);")
        .unwrap();

    // TODO: Is there any way to insert multiple rows at once?
    for (source, target) in sqlite_rx {
        insert.execute((source.as_str(), target.as_str())).unwrap();
    }

    println!("[sqlite thread] Creating index");
    sqlite
        .execute_batch(
            "
            BEGIN;

            CREATE INDEX IF NOT EXISTS index_vertices ON vertices(source, target);

            COMMIT;
            ",
        )
        .unwrap();

    println!("[sqlite thread] Exiting");
}
