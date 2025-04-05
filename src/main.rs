mod account;
mod declare;
mod deploy_account;
mod deploy_erc20;
mod transfert;

use std::error::Error;
use std::sync::Arc;

use account::AccountManager;
use futures::StreamExt;
use starknet::{
    accounts::OpenZeppelinAccountFactory,
    core::types::{BlockId, Felt},
    providers::{
        jsonrpc::{HttpTransport, JsonRpcClient},
        Provider, Url,
    },
    signers::{LocalWallet, SigningKey},
};

pub static CHAIN_ID: Felt = Felt::from_hex_unchecked("0x4d41444152415f4445564e4554"); // MADARA_DEVNET
pub static MAX_FEE: Felt = Felt::from_hex_unchecked("0x6efb28c75a0000");
pub static FEE_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7");

pub static PRIVATE_KEY: Felt =
    Felt::from_hex_unchecked("0x6b80ef06aab04d361b1458a4f2125067d04c4849d43871669a0ad3616a41c4");
pub static ADDRESS: Felt =
    Felt::from_hex_unchecked("0x3050c0b1b7db750b7ec4c23bcd7db958efccb9aaf682d3f91a5ee13e7ce2e9a");

pub static NB_ACCOUNTS: i32 = 1000;

#[tokio::main]
async fn main() {
    let provider: Arc<JsonRpcClient<HttpTransport>> = Arc::new(JsonRpcClient::new(
        HttpTransport::new(Url::parse("http://localhost:9944/").unwrap()),
    ));

    let nonce = provider
        .get_nonce(
            BlockId::Tag(starknet::core::types::BlockTag::Pending),
            ADDRESS,
        )
        .await
        .unwrap();

    println!("Nonce: {}", nonce);

    let dev_account = AccountManager::new(
        provider.clone(),
        PRIVATE_KEY,
        &ADDRESS,
        nonce.to_string().parse::<u64>().unwrap(),
        starknet::accounts::ExecutionEncoding::New,
    );

    let signer = LocalWallet::from(SigningKey::from_secret_scalar(PRIVATE_KEY));

    // let oz_class_hash = dev_account
    //     .declare_legacy("./contracts/v0/OpenzeppelinAccount.json")
    //     .await
    //     .unwrap();

    let oz_class_hash = Felt::from_hex_unchecked(
        "0x045e3ac97b1c575540dbf6b6f089f390f00fa98928415bb91a27a43790b52f30",
    );

    println!("OpenZeppelinAccount class hash: 0x{:x}", oz_class_hash);

    // let erc_20_class_hash = dev_account
    //     .declare_legacy("./contracts/v0/ERC20.json")
    //     .await
    //     .unwrap();

    let erc_20_class_hash = Felt::from_hex_unchecked(
        "0x0372ee6669dc86563007245ed7343d5180b96221ce28f44408cff2898038dbd4",
    );

    println!("ERC20 class hash: 0x{:x}", erc_20_class_hash);

    println!("Waiting for the contract to be declared...");
    // tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let erc20_address = dev_account
        .deploy_erc20(
            erc_20_class_hash,
            "Test",
            "T",
            18,
            100_000_000.into(),
            ADDRESS,
            0.into(),
        )
        .await
        .unwrap();

    println!("ERC20 address: 0x{:x}", erc20_address);

    let account_factory =
        OpenZeppelinAccountFactory::new(oz_class_hash, CHAIN_ID, &signer, &provider)
            .await
            .unwrap();

    let base_num = 10000 * (100 + rand::random::<u32>() % 9900) as i32;
    let addresses = (0..NB_ACCOUNTS)
        .map(|i| deploy_account::get_address(&account_factory, (i + base_num).into()))
        .collect::<Vec<_>>();

    let accounts = addresses
        .iter()
        .map(|address| {
            AccountManager::new(
                provider.clone(),
                PRIVATE_KEY,
                address,
                1,
                starknet::accounts::ExecutionEncoding::Legacy,
            )
        })
        .collect::<Vec<_>>();

    for account in accounts.iter() {
        dev_account
            .transfer(
                &erc20_address,
                &Felt::from_hex_unchecked("0x2710"),
                &account.address(),
            )
            .await
            .unwrap();
        println!("🏛️ Transferred 10 MAX_FEE to 0x{:x}", account.address());
    }

    println!("Waiting for the accounts to be funded for deploying...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let accounts_addresses = futures::stream::iter(0..NB_ACCOUNTS)
        .map(|i| deploy_account::deploy_account(&account_factory, (i + base_num).into()))
        .buffer_unordered(100)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("Waiting for the accounts to be deployed... Press Enter to continue.");
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");

    let accounts = Arc::new(accounts);
    loop_transfers(accounts.clone(), 500, 100000, erc20_address).await;
}

pub async fn loop_transfers_tokio(
    accounts: Arc<Vec<AccountManager>>,
    concurrency: usize,
    num_iterations: usize,
    erc20_address: Felt,
) {
}

// Every iteration does a transfer from all accounts
// Concurrency is the number of transfers done in parallel when sending one iteration
pub async fn loop_transfers(
    accounts: Arc<Vec<AccountManager>>,
    concurrency: usize,
    num_iterations: usize,
    erc20_address: Felt,
) {
    let total_accounts = accounts.len();
    let offset = total_accounts / 2;

    println!(
        "Starting loop transfers with {} accounts, {} iterations",
        total_accounts, num_iterations
    );

    let mut all_transfers = Vec::new();

    for sender_idx in 0..total_accounts {
        let recipient_idx = (sender_idx + offset) % total_accounts;
        all_transfers.push((sender_idx, recipient_idx));
    }

    println!("Prepared {} total transfers", all_transfers.len());

    let mut success_count = 0;
    let mut failed_count = 0;
    let start_time = std::time::Instant::now();

    let total_transfers = num_iterations * total_accounts;
    for batch_idx in 0..((total_transfers + concurrency - 1) / concurrency) {
        let start_idx = batch_idx * concurrency % all_transfers.len();
        let end_idx = std::cmp::min(start_idx + concurrency, total_transfers);
        let batch: &[(usize, usize)] = &all_transfers[start_idx..end_idx];

        let chunk_start = std::time::Instant::now();
        println!(
            "Processing batch {}/{} ({} transfers)...",
            batch_idx + 1,
            (total_transfers + concurrency - 1) / concurrency,
            batch.len()
        );

        let futures = batch.iter().map(|(sender_idx, recipient_idx)| {
            let accounts = accounts.clone();
            let s_idx = *sender_idx;
            let r_idx = *recipient_idx;

            async move {
                let sender = &accounts[s_idx];
                let recipient_address = accounts[r_idx].address();
                let amount = Felt::from(0);

                match sender
                    .transfer(
                        &erc20_address,
                        &amount,
                        // &Felt::from_dec_str(&(batch_idx * 100000 + iter + 1).to_string()).unwrap(),
                        &recipient_address,
                    )
                    .await
                {
                    Ok(tx_hash) => {
                        println!(
                            "✅ Iter {} | {}->{}: tx {:#064x}",
                            batch_idx, s_idx, r_idx, tx_hash
                        );
                        Ok((s_idx, r_idx))
                    }
                    Err(e) => {
                        eprintln!(
                            "❌ Iter {} | {}->{}: erreur: {}",
                            batch_idx + 1,
                            s_idx,
                            r_idx,
                            e
                        );
                        Err(e)
                    }
                }
            }
        });

        let results: Vec<Result<(usize, usize), Box<dyn Error>>> = futures::stream::iter(futures)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let batch_success = results.iter().filter(|r| r.is_ok()).count();
        let batch_failed = results.len() - batch_success;

        success_count += batch_success;
        failed_count += batch_failed;

        let elapsed = chunk_start.elapsed();
        let tps = batch.len() as f64 / elapsed.as_secs_f64();

        println!(
            "Batch {}: {}/{} successful ({:.1} TPS)",
            batch_idx + 1,
            batch_success,
            batch.len(),
            tps
        );
    }

    let total_elapsed = start_time.elapsed();
    let overall_tps = (success_count + failed_count) as f64 / total_elapsed.as_secs_f64();

    println!(
        "Transfers completed: {}/{} successful in {:?} ({:.1} TPS)",
        success_count,
        success_count + failed_count,
        total_elapsed,
        overall_tps
    );
}
