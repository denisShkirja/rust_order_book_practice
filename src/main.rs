use clap::Parser;
use std::fmt::Debug;
use std::fs::File;
use std::path::PathBuf;

mod l2_order_book;
mod parsing;

use binread::BinRead;
use l2_order_book::errors::Errors as OrderBookErrors;
use l2_order_book::manager::Manager as OrderBookManager;
use parsing::binary_file_iterator::BinaryFileIterator;
use parsing::order_book_snapshot::OrderBookSnapshot;
use parsing::order_book_update::OrderBookUpdate;
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[clap(about = "Processes snapshot and incremental files")]
struct Args {
    path_to_snapshot: PathBuf,
    path_to_incremental: PathBuf,
    #[clap(short, long, help = "Enable verbose output")]
    verbose: bool,
}

fn print_records_from_file<T: BinRead + Debug>(path: &PathBuf) {
    println!("Printing records from file: {}", path.display());
    let file = File::open(path);
    if file.is_err() {
        eprintln!("Failed to open file: {}", path.display());
        return;
    }

    let mut record_count = 0;
    for record in BinaryFileIterator::<T>::new(file.unwrap()) {
        match record {
            Ok(record) => {
                println!("{:#?}", &record);
                record_count += 1;
            }
            Err(e) => {
                eprintln!(
                    "Failed to read next record from the file: {}. The file is corrupted.",
                    e
                );
                return;
            }
        }
    }
    println!("Successfully read {} records from the file", record_count);
}

trait ApplyToOrderBook {
    fn apply_to_order_book(self, manager: &mut OrderBookManager) -> Result<(), OrderBookErrors>;
    fn get_record_type() -> &'static str;
}

impl ApplyToOrderBook for OrderBookSnapshot {
    fn apply_to_order_book(self, manager: &mut OrderBookManager) -> Result<(), OrderBookErrors> {
        manager.apply_snapshot(&self)
    }

    fn get_record_type() -> &'static str {
        "Snapshot"
    }
}

impl ApplyToOrderBook for OrderBookUpdate {
    fn apply_to_order_book(self, manager: &mut OrderBookManager) -> Result<(), OrderBookErrors> {
        manager.apply_update(self)
    }

    fn get_record_type() -> &'static str {
        "Update"
    }
}

fn apply_order_book_records_from_file<T: BinRead + Debug + ApplyToOrderBook>(
    path: &PathBuf,
    order_book_manager: &mut OrderBookManager,
) -> bool {
    let file = File::open(path);
    if file.is_err() {
        eprintln!("Failed to open file: {}", path.display());
        return false;
    }

    for record in BinaryFileIterator::<T>::new(file.unwrap()) {
        match record {
            Ok(record) => {
                if let Err(e) = record.apply_to_order_book(order_book_manager) {
                    match e {
                        OrderBookErrors::InvalidPrice(update_msg_info, msg) => {
                            eprintln!(
                                "{} for security {} with seq_no {} has invalid price: {}. The record will be ignored.",
                                T::get_record_type(),
                                update_msg_info.security_id,
                                update_msg_info.seq_no,
                                msg
                            );
                        }
                        OrderBookErrors::InvalidSide(update_msg_info, msg) => {
                            eprintln!(
                                "{} for security {} with seq_no {} has invalid side: {}. The record will be ignored.",
                                T::get_record_type(),
                                update_msg_info.security_id,
                                update_msg_info.seq_no,
                                msg
                            );
                        }
                        OrderBookErrors::SecurityIdMismatch => {
                            eprintln!("Internal error: Security ID mismatch.");
                        }
                        OrderBookErrors::OrderBookNotFound => {}
                        OrderBookErrors::SequenceNumberGap => {}
                        OrderBookErrors::OldSequenceNumber => {}
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to read next {} from the file: {}. The file {} is corrupted.",
                    T::get_record_type(),
                    e,
                    path.display()
                );
                return true;
            }
        }
    }
    true
}

fn main() -> ExitCode {
    let args = Args::parse();

    if args.verbose {
        print_records_from_file::<OrderBookSnapshot>(&args.path_to_snapshot);
        print_records_from_file::<OrderBookUpdate>(&args.path_to_incremental);
    }

    let mut order_book_manager = OrderBookManager::default();

    // Process snapshot file
    if !apply_order_book_records_from_file::<OrderBookSnapshot>(
        &args.path_to_snapshot,
        &mut order_book_manager,
    ) {
        return ExitCode::FAILURE;
    }

    // Process incremental file
    if !apply_order_book_records_from_file::<OrderBookUpdate>(
        &args.path_to_incremental,
        &mut order_book_manager,
    ) {
        return ExitCode::FAILURE;
    }

    // Print all order books
    print!("{}", order_book_manager);

    ExitCode::SUCCESS
}
