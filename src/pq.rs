//! Minimal Parquet writer: one row group per file, one data page (v1) per column,
//! no compression. Pages are encoded straight from caller-supplied iterators into
//! the file buffer (no intermediate column vectors). Supports required BYTE_ARRAY
//! (UTF8, PLAIN), INT32 (plain or DATE), INT64, and optional dictionary-encoded
//! BYTE_ARRAY (UTF8, RLE_DICTIONARY with a PLAIN dictionary page).

// Thrift compact protocol type ids
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn zz32(v: i32) -> u64 {
    ((v << 1) ^ (v >> 31)) as u32 as u64
}

fn zz64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

struct Thrift<'b> {
    buf: &'b mut Vec<u8>,
    last: Vec<i16>,
}

impl<'b> Thrift<'b> {
    fn new(buf: &'b mut Vec<u8>) -> Self {
        Thrift { buf, last: vec![0] }
    }
    fn header(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            varint(self.buf, zz32(id as i32));
        }
        *last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.header(id, T_I32);
        varint(self.buf, zz32(v));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.header(id, T_I64);
        varint(self.buf, zz64(v));
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.header(id, T_BINARY);
        varint(self.buf, v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn struct_begin(&mut self, id: i16) {
        self.header(id, T_STRUCT);
        self.last.push(0);
    }
    fn struct_end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list_begin(&mut self, id: i16, elem: u8, n: usize) {
        self.header(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            varint(self.buf, n as u64);
        }
    }
    // element of a list of structs
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
    fn elem_end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    DictStr,
    Date,
    I32,
    I64,
}

struct ChunkMeta {
    name: &'static str,
    kind: Kind,
    size: i64,
    /// start of the column chunk (dictionary page if any, else the data page)
    offset: i64,
    /// start of the data page
    data_offset: i64,
}

pub struct Writer<'a> {
    out: Vec<u8>,
    rows: usize,
    metas: Vec<ChunkMeta>,
    /// caller-owned page bodies spliced into the file: (position in `out`, bytes)
    segs: Vec<(usize, &'a [u8])>,
    ext: usize, // total length of segs
}

/// Bytes of a plain little-endian column (the PLAIN encoding of INT32 / INT64 on x86).
pub fn le_bytes<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

impl<'a> Writer<'a> {
    /// File offset of the next byte.
    fn pos(&self) -> usize {
        self.out.len() + self.ext
    }

    /// Required column whose PLAIN-encoded page body the caller already holds (`kind`: 0 UTF8
    /// with 4-byte length prefixes, 1 DATE, 2 INT32, 3 INT64). The body is not copied; write
    /// the file with `finish_parts`.
    pub fn raw_col(&mut self, name: &'static str, kind: u8, page: &'a [u8]) {
        let kind = match kind {
            0 => Kind::Str,
            1 => Kind::Date,
            2 => Kind::I32,
            _ => Kind::I64,
        };
        match kind {
            Kind::Date | Kind::I32 => assert_eq!(page.len(), self.rows * 4),
            Kind::I64 => assert_eq!(page.len(), self.rows * 8),
            _ => {}
        }
        let offset = self.pos();
        self.data_page_header(page.len(), 0);
        self.segs.push((self.out.len(), page));
        self.ext += page.len();
        self.push_meta(name, kind, offset, offset);
    }

    /// Append the footer and return the file as a sequence of pieces to write in order.
    pub fn finish_parts(mut self) -> (Vec<u8>, Vec<(usize, &'a [u8])>) {
        let segs = std::mem::take(&mut self.segs);
        self.ext = 0;
        (self.finish(), segs)
    }

    /// `rows` is the row count of every column; `cap` a capacity hint in bytes.
    pub fn new(rows: usize, cap: usize) -> Self {
        let mut out: Vec<u8> = Vec::with_capacity(cap.max(1 << 10));
        out.extend_from_slice(b"PAR1");
        Writer { out, rows, metas: Vec::new(), segs: Vec::new(), ext: 0 }
    }

    fn data_page_header(&mut self, page_len: usize, encoding: i32) {
        let rows = self.rows as i32;
        let mut t = Thrift::new(&mut self.out);
        t.i32(1, 0); // DATA_PAGE
        t.i32(2, page_len as i32);
        t.i32(3, page_len as i32);
        t.struct_begin(5);
        t.i32(1, rows);
        t.i32(2, encoding);
        t.i32(3, 3); // RLE
        t.i32(4, 3); // RLE
        t.struct_end();
        t.buf.push(0); // end PageHeader
    }

    fn push_meta(&mut self, name: &'static str, kind: Kind, offset: usize, data_offset: usize) {
        self.metas.push(ChunkMeta {
            name,
            kind,
            size: (self.pos() - offset) as i64,
            offset: offset as i64,
            data_offset: data_offset as i64,
        });
    }

    /// Required UTF8 column, PLAIN.
    pub fn str_col<'s, I>(&mut self, name: &'static str, it: I)
    where
        I: Iterator<Item = &'s [u8]> + Clone,
    {
        let page_len: usize = it.clone().map(|s| s.len() + 4).sum();
        let offset = self.pos();
        self.out.reserve(page_len + 64);
        self.data_page_header(page_len, 0);
        let start = self.out.len();
        let mut n = 0usize;
        for s in it {
            self.out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            self.out.extend_from_slice(s);
            n += 1;
        }
        assert_eq!(n, self.rows);
        assert_eq!(self.out.len() - start, page_len);
        self.push_meta(name, Kind::Str, offset, offset);
    }

    fn i32_any<I: Iterator<Item = i32>>(&mut self, name: &'static str, kind: Kind, it: I) {
        let page_len = self.rows * 4;
        let offset = self.pos();
        self.out.reserve(page_len + 64);
        self.data_page_header(page_len, 0);
        let start = self.out.len();
        self.out.resize(start + page_len, 0);
        let mut n = 0usize;
        for (c, v) in self.out[start..].chunks_exact_mut(4).zip(it) {
            c.copy_from_slice(&v.to_le_bytes());
            n += 1;
        }
        assert_eq!(n, self.rows);
        self.push_meta(name, kind, offset, offset);
    }

    /// Required INT32 column with the DATE logical type (days since epoch).
    pub fn date_col<I: Iterator<Item = i32>>(&mut self, name: &'static str, it: I) {
        self.i32_any(name, Kind::Date, it);
    }

    /// Required INT32 column.
    pub fn i32_col<I: Iterator<Item = i32>>(&mut self, name: &'static str, it: I) {
        self.i32_any(name, Kind::I32, it);
    }

    /// Required INT64 column.
    pub fn i64_col<I: Iterator<Item = i64>>(&mut self, name: &'static str, it: I) {
        let page_len = self.rows * 8;
        let offset = self.pos();
        self.out.reserve(page_len + 64);
        self.data_page_header(page_len, 0);
        let start = self.out.len();
        self.out.resize(start + page_len, 0);
        let mut n = 0usize;
        for (c, v) in self.out[start..].chunks_exact_mut(8).zip(it) {
            c.copy_from_slice(&v.to_le_bytes());
            n += 1;
        }
        assert_eq!(n, self.rows);
        self.push_meta(name, Kind::I64, offset, offset);
    }

    /// Optional UTF8 column, dictionary encoded (RLE_DICTIONARY, PLAIN dictionary
    /// page). The iterator yields indexes into `dict`; `u32::MAX` means null.
    pub fn dict_col<I>(&mut self, name: &'static str, dict: &[&[u8]], it: I)
    where
        I: Iterator<Item = u32> + Clone,
    {
        let rows = self.rows;
        let offset = self.pos();
        // dictionary page (PLAIN)
        let dict_len: usize = dict.iter().map(|s| s.len() + 4).sum();
        {
            let mut t = Thrift::new(&mut self.out);
            t.i32(1, 2); // DICTIONARY_PAGE
            t.i32(2, dict_len as i32);
            t.i32(3, dict_len as i32);
            t.struct_begin(7);
            t.i32(1, dict.len() as i32);
            t.i32(2, 0); // PLAIN
            t.struct_end();
            t.buf.push(0);
        }
        for s in dict {
            self.out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            self.out.extend_from_slice(s);
        }
        // data page: definition levels (4-byte length + one bit-packed run, width 1),
        // then the index bit width and one bit-packed run of the non-null indexes.
        let bw = (32 - (dict.len().max(2) as u32 - 1).leading_zeros()) as usize;
        let nn = it.clone().filter(|&v| v != u32::MAX).count();
        let dgroups = (rows + 7) / 8;
        let igroups = (nn + 7) / 8;
        let mut dhdr = Vec::with_capacity(10);
        varint(&mut dhdr, ((dgroups as u64) << 1) | 1);
        let mut ihdr = Vec::with_capacity(11);
        ihdr.push(bw as u8);
        if igroups > 0 {
            varint(&mut ihdr, ((igroups as u64) << 1) | 1);
        }
        let def_len = dhdr.len() + dgroups;
        let page_len = 4 + def_len + ihdr.len() + igroups * bw;
        let data_offset = self.pos();
        self.out.reserve(page_len + 64);
        self.data_page_header(page_len, 8); // RLE_DICTIONARY
        self.out.extend_from_slice(&(def_len as u32).to_le_bytes());
        self.out.extend_from_slice(&dhdr);
        let dstart = self.out.len();
        self.out.resize(dstart + dgroups, 0);
        self.out.extend_from_slice(&ihdr);
        let istart = self.out.len();
        self.out.resize(istart + igroups * bw, 0);
        let (head, idx) = self.out.split_at_mut(istart);
        let def = &mut head[dstart..dstart + dgroups];
        let mut acc = 0u64;
        let mut nbits = 0usize;
        let mut ip = 0usize;
        let mut n = 0usize;
        for (i, v) in it.enumerate() {
            n += 1;
            if v == u32::MAX {
                continue;
            }
            assert!((v as usize) < dict.len());
            def[i >> 3] |= 1 << (i & 7);
            acc |= (v as u64) << nbits;
            nbits += bw;
            while nbits >= 8 {
                idx[ip] = acc as u8;
                ip += 1;
                acc >>= 8;
                nbits -= 8;
            }
        }
        if nbits > 0 {
            idx[ip] = acc as u8;
        }
        assert_eq!(n, rows);
        self.push_meta(name, Kind::DictStr, offset, data_offset);
    }

    /// Append the footer and return the complete Parquet file.
    pub fn finish(self) -> Vec<u8> {
        let Writer { mut out, rows, metas, segs, ext: _ } = self;
        assert!(segs.is_empty(), "raw columns need finish_parts");
        let meta_start = out.len();
        {
            let mut t = Thrift::new(&mut out);
            t.i32(1, 1); // version
            t.list_begin(2, T_STRUCT, metas.len() + 1);
            // root
            t.elem_begin();
            t.bin(4, b"schema");
            t.i32(5, metas.len() as i32);
            t.elem_end();
            for m in metas.iter() {
                let ptype = match m.kind {
                    Kind::Str | Kind::DictStr => 6,
                    Kind::Date | Kind::I32 => 1,
                    Kind::I64 => 2,
                };
                t.elem_begin();
                t.i32(1, ptype);
                t.i32(3, if m.kind == Kind::DictStr { 1 } else { 0 });
                t.bin(4, m.name.as_bytes());
                match m.kind {
                    Kind::Str | Kind::DictStr => {
                        t.i32(6, 0); // converted UTF8
                        t.struct_begin(10); // logicalType
                        t.struct_begin(1); // STRING
                        t.struct_end();
                        t.struct_end();
                    }
                    Kind::Date => {
                        t.i32(6, 6); // converted DATE
                        t.struct_begin(10);
                        t.struct_begin(6); // DATE
                        t.struct_end();
                        t.struct_end();
                    }
                    Kind::I32 | Kind::I64 => {}
                }
                t.elem_end();
            }
            t.i64(3, rows as i64);
            t.list_begin(4, T_STRUCT, 1);
            t.elem_begin(); // RowGroup
            t.list_begin(1, T_STRUCT, metas.len());
            let mut total = 0i64;
            for m in metas.iter() {
                let ptype = match m.kind {
                    Kind::Str | Kind::DictStr => 6,
                    Kind::Date | Kind::I32 => 1,
                    Kind::I64 => 2,
                };
                let dict = m.kind == Kind::DictStr;
                total += m.size;
                t.elem_begin(); // ColumnChunk
                t.i64(2, m.offset);
                t.struct_begin(3); // ColumnMetaData
                t.i32(1, ptype);
                t.list_begin(2, T_I32, if dict { 3 } else { 2 });
                varint(t.buf, zz32(0)); // PLAIN
                varint(t.buf, zz32(3)); // RLE
                if dict {
                    varint(t.buf, zz32(8)); // RLE_DICTIONARY
                }
                t.list_begin(3, T_BINARY, 1);
                varint(t.buf, m.name.len() as u64);
                t.buf.extend_from_slice(m.name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, rows as i64);
                t.i64(6, m.size);
                t.i64(7, m.size);
                t.i64(9, m.data_offset);
                if dict {
                    t.i64(11, m.offset);
                }
                t.struct_end();
                t.elem_end();
            }
            t.i64(2, total);
            t.i64(3, rows as i64);
            t.elem_end();
            t.bin(6, b"sessions-engine");
            t.buf.push(0); // end FileMetaData
        }
        let meta_len = (out.len() - meta_start) as u32;
        out.extend_from_slice(&meta_len.to_le_bytes());
        out.extend_from_slice(b"PAR1");
        out
    }
}
