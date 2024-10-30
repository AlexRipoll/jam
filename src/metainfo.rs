#[derive(Debug)]
struct Metainfo {
    info: Info,
    announce: String,
    announce_list: Option<Vec<Vec<String>>>,
    creation_date: Option<u64>,
    comment: Option<String>,
    created_by: Option<String>,
    encoding: Option<String>,
}

#[derive(Debug)]
struct InfoSingle {
    name: String,
    length: u64,
    piece_length: u64,
    pieces: String,
    private: Option<u8>,
    md5sum: Option<String>,
}

#[derive(Debug)]
struct InfoMulti {
    name: String,
    length: u64,
    piece_length: u64,
    pieces: Vec<u8>,
    private: Option<bool>,
    files: Vec<Files>,
}

#[derive(Debug)]
struct Files {
    length: u64,
    md5sum: Option<String>,
    path: Vec<String>,
}

#[derive(Debug)]
enum Info {
    Single(InfoSingle),
    Multi(InfoMulti),
}
