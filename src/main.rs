#![allow(dead_code)]
#![allow(unused_variables)]

use anyhow::Context as _;
use clap::Parser as _;
use quick_xml::events::Event;
use std::{fs::File, io::BufReader, path::PathBuf};

#[derive(clap::Parser)]
struct Args {
    /// Wikipedia dump file (multistream `*.xml.bz2`)
    input: PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let mut xml = read_xml(&args.input).context("Failed to read XML file")?;

    let mut count = 0;

    while count < 10 {
        let page = read_page(&mut xml).context("Failed to read page")?;
        println!("{page:?}");
        count += 1;
    }

    Ok(())
}

enum Xml {
    Raw(quick_xml::Reader<BufReader<File>>),
    Bzip2(quick_xml::Reader<BufReader<bzip2::read::BzDecoder<File>>>),
    MultistreamBzip2(quick_xml::Reader<BufReader<bzip2::read::MultiBzDecoder<File>>>),
}

fn read_xml(path: &PathBuf) -> anyhow::Result<Xml> {
    if !path.is_file() {
        anyhow::bail!("Path is not a file");
    }

    let file_name = path
        .file_name()
        .context("Could not get file name from path")?
        .to_str()
        .context("File name is not valid UTF-8")?;

    let mut file_name = String::from(file_name);
    file_name.make_ascii_lowercase();

    if file_name.ends_with("multistream.xml.bz2") {
        tracing::info!("Reading '{}' as multistream bzip2 XML", path.display());
        let file = File::open(path)?;
        let bzip2_decoder = bzip2::read::MultiBzDecoder::new(file);
        let buf_reader = BufReader::new(bzip2_decoder);
        let xml_reader = quick_xml::Reader::from_reader(buf_reader);
        Ok(Xml::MultistreamBzip2(xml_reader))
    } else if file_name.ends_with(".xml.bz2") {
        tracing::info!("Reading '{}' as bzip2 XML", path.display());
        let file = File::open(path)?;
        let bzip2_decoder = bzip2::read::BzDecoder::new(file);
        let buf_reader = BufReader::new(bzip2_decoder);
        let xml_reader = quick_xml::Reader::from_reader(buf_reader);
        Ok(Xml::Bzip2(xml_reader))
    } else {
        tracing::info!("Reading '{}' as raw XML", path.display());
        let xml_reader = quick_xml::Reader::from_file(path)?;
        Ok(Xml::Raw(xml_reader))
    }
}

#[derive(Debug)]
struct Page {
    title: String,
    text: String,
}

#[derive(Clone, Debug)]
enum State {
    Limbo1,
    TitleStarted,
    Title { title: String },
    Limbo2 { title: String },
    TextStarted { title: String },
    Text { title: String, text: String },
}

fn read_page(xml: &mut Xml) -> anyhow::Result<Option<Page>> {
    let mut buffer = Vec::new();
    let mut state = State::Limbo1;

    loop {
        let event = (match xml {
            Xml::Raw(xml) => xml.read_event_into(&mut buffer),
            Xml::Bzip2(xml) => xml.read_event_into(&mut buffer),
            Xml::MultistreamBzip2(xml) => xml.read_event_into(&mut buffer),
        })
        .context("Failed to read XML event")?;

        match (state, event) {
            (State::Limbo1, Event::Eof) => {
                return Ok(None);
            }
            (State::Limbo1, Event::Start(data)) if data.name().into_inner() == b"title" => {
                state = State::TitleStarted;
            }
            (limbo1 @ State::Limbo1, _) => {
                state = limbo1;
            }
            (State::TitleStarted, Event::Text(data)) => {
                let title = data.unescape()?.into_owned();
                state = State::Title { title };
            }
            (State::Title { title }, Event::End(data)) if data.name().into_inner() == b"title" => {
                state = State::Limbo2 { title };
            }
            (State::Limbo2 { title }, Event::Start(data))
                if data.name().into_inner() == b"text" =>
            {
                state = State::TextStarted { title };
            }
            (limbo2 @ State::Limbo2 { .. }, _) => {
                state = limbo2;
            }
            (State::TextStarted { title }, Event::Text(data)) => {
                let text = data.unescape()?.into_owned();
                state = State::Text { title, text };
            }
            (State::Text { title, text }, Event::End(data))
                if data.name().into_inner() == b"text" =>
            {
                return Ok(Some(Page { title, text }));
            }
            (state, event) => anyhow::bail!("Unexpected event `{event:?}` in state `{state:?}`",),
        }

        buffer.clear();
    }
}
