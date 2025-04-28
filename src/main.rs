use ethers::prelude::*;
use ethers::types::{Address, Transaction, H256};
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::{sleep, timeout};
use teloxide::Bot;
use teloxide::prelude::{Requester, Request};
use teloxide::types::{ChatId, ParseMode};
use teloxide::payloads::SendMessageSetters;
use dotenv::dotenv;
use std::env;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use hex::decode;
use chrono::Local;
use log::{error, info, warn};
use env_logger;
use ssh2::Session;
use std::net::TcpStream;
use backoff::{future::retry, ExponentialBackoff, Error as BackoffError};
use futures::stream::StreamExt;
use tokio::task;
use tokio::task::JoinSet;
use tokio::signal;
use lru::LruCache;
use anyhow::Result;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;

// Custom writer to tee logs to stdout and file
struct TeeWriter {
    file: Arc<Mutex<File>>,
}

impl Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::stdout().write(buf)?;
        let mut file = self.file.lock().unwrap();
        file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()?;
        self.file.lock().unwrap().flush()?;
        Ok(())
    }
}

// Constants
const TELEGRAM_CHAT_ID: i64 = -61;
const PROJECT_DIR: &str = "projects";
const ITYFUZZ_PATH: &str = "ityfuzz";
const ITYFUZZ_INSTALL_SCRIPT: &str = r#"curl -L https://ity.fuzz.land/ | bash && ityfuzzup"#;
const MAX_CONCURRENCY: usize = 6;
const TASKS_PER_HOST: usize = 1;
const API_RATE_LIMIT: Duration = Duration::from_millis(200);
const NO_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const ITYFUZZ_TIMEOUT: Duration = Duration::from_secs(60 * 60); // Increased to 1 hour
const CONTRACT_CACHE_SIZE: usize = 10000;
const SSH_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(60);
const PENDING_CHECK_INTERVAL: Duration = Duration::from_secs(15);
const DEPLOYED_CHECK_INTERVAL: Duration = Duration::from_secs(24 * 3600);
const DATABASE_CLEANUP_INTERVAL: Duration = Duration::from_secs(7 * 24 * 3600);
const ETHERSCAN_API_KEY: &str = "Ywyw";
const BATCH_PROCESS_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BATCH_SIZE: usize = 10;
const API_BACKOFF_DURATION: Duration = Duration::from_millis(200);
const CAPACITY_WAIT_DURATION: Duration = Duration::from_secs(60);
const TASK_SPAWN_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_ETH_RPC_URL: &str = "ws://127.0.0.1:8546";

// Stablecoin addresses (Ethereum)
const STABLECOINS: &[&str] = &[
    "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH
];

// Configuration struct
#[derive(Clone)]
struct Config {
    infura_api_key: String,
    ssh_hosts: Vec<(String, String)>,
    ssh_user: String,
    hot_wallet_threshold: u32,
    hot_wallet_window: u64,
    wallet_private_key: String,
    log_file: String,
    database_file: String,
    telegram_bot_token: String,
    telegram_chat_id: i64,
}

// SQLite databases
#[derive(Clone)]
struct Database {
    scanned: Arc<Mutex<Connection>>,
    deployed: Arc<Mutex<Connection>>,
}

impl Database {
    fn new(scanned_path: &str, deployed_path: &str) -> SqliteResult<Self> {
        let scanned = Connection::open(scanned_path)?;
        scanned.execute(
            "CREATE TABLE IF NOT EXISTS scanned_contracts (
                address TEXT PRIMARY KEY,
                pending_addresses TEXT,
                processed INTEGER,
                scanned_at TEXT
            )",
            [],
        )?;
        scanned.execute(
            "CREATE INDEX IF NOT EXISTS idx_address ON scanned_contracts(address)",
            [],
        )?;
        scanned.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed ON scanned_contracts(processed)",
            [],
        )?;
        scanned.execute(
            "CREATE INDEX IF NOT EXISTS idx_scanned_at ON scanned_contracts(scanned_at)",
            [],
        )?;
        scanned.execute(
            "CREATE TABLE IF NOT EXISTS etherscan_cache (
                address TEXT PRIMARY KEY,
                contracts TEXT,
                cached_at TEXT
            )",
            [],
        )?;
        scanned.execute(
            "CREATE INDEX IF NOT EXISTS idx_cached_at ON etherscan_cache(cached_at)",
            [],
        )?;

        let deployed = Connection::open(deployed_path)?;
        deployed.execute(
            "CREATE TABLE IF NOT EXISTS deployed_contracts (
                address TEXT PRIMARY KEY,
                deployed_at TEXT,
                last_checked TEXT,
                tx_count INTEGER
            )",
            [],
        )?;
        deployed.execute(
            "CREATE INDEX IF NOT EXISTS idx_address ON deployed_contracts(address)",
            [],
        )?;

        Ok(Database {
            scanned: Arc::new(Mutex::new(scanned)),
            deployed: Arc::new(Mutex::new(deployed)),
        })
    }

    fn is_scanned(&self, address: &str) -> SqliteResult<bool> {
        let conn = self.scanned.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT processed FROM scanned_contracts WHERE address = ?",
        )?;
        let processed = stmt.query_row([address], |row| row.get::<_, i32>(0));
        Ok(processed.map(|p| p == 1).unwrap_or(false))
    }

    fn save_contract(
        &self,
        address: &str,
        pending_addresses: &str,
        processed: bool,
    ) -> SqliteResult<()> {
        let conn = self.scanned.lock().unwrap();
        let result = conn.execute(
            "INSERT OR REPLACE INTO scanned_contracts (address, pending_addresses, processed, scanned_at) VALUES (?, ?, ?, ?)",
            (
                &address,
                &pending_addresses,
                if processed { 1 } else { 0 },
                Local::now().to_rfc3339(),
            ),
        );
        match result {
            Ok(_) => {
                info!("Saved contract {} to database (processed: {})", address, processed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn get_pending_contracts(&self) -> SqliteResult<Vec<(String, String)>> {
        let conn = self.scanned.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT address, pending_addresses FROM scanned_contracts WHERE processed = 0",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        info!("Fetched {} pending contracts", results.len());
        Ok(results)
    }

    fn mark_processed(&self, address: &str) -> SqliteResult<()> {
        let conn = self.scanned.lock().unwrap();
        conn.execute(
            "UPDATE scanned_contracts SET processed = 1, scanned_at = ? WHERE address = ?",
            (Local::now().to_rfc3339(), address),
        )?;
        info!("Marked contract {} as processed", address);
        Ok(())
    }

    fn save_deployed_contract(&self, address: &str) -> SqliteResult<()> {
        let conn = self.deployed.lock().unwrap();
        let result = conn.execute(
            "INSERT OR IGNORE INTO deployed_contracts (address, deployed_at, last_checked, tx_count) VALUES (?, ?, ?, ?)",
            (
                &address,
                Local::now().to_rfc3339(),
                Local::now().to_rfc3339(),
                0,
            ),
        );
        match result {
            Ok(_) => {
                info!("Saved deployed contract {}", address);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn get_deployed_contracts(&self) -> SqliteResult<Vec<(String, String, i32)>> {
        let conn = self.deployed.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT address, last_checked, tx_count FROM deployed_contracts",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    fn update_deployed_contract(&self, address: &str, tx_count: i32) -> SqliteResult<()> {
        let conn = self.deployed.lock().unwrap();
        conn.execute(
            "UPDATE deployed_contracts SET last_checked = ?, tx_count = ? WHERE address = ?",
            (Local::now().to_rfc3339(), tx_count, address),
        )?;
        info!("Updated deployed contract {} with tx_count {}", address, tx_count);
        Ok(())
    }

    fn get_cached_contracts(&self, address: &str) -> SqliteResult<Option<Vec<Address>>> {
        let conn = self.scanned.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT contracts, cached_at FROM etherscan_cache WHERE address = ?",
        )?;
        let result = stmt.query_row([address], |row| {
            let contracts: String = row.get(0)?;
            let cached_at: String = row.get(1)?;
            let cached_time = chrono::DateTime::parse_from_rfc3339(&cached_at)
                .map(|dt| dt.timestamp())
                .unwrap_or(0);
            if SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - cached_time
                > 24 * 3600
            {
                return Ok(None);
            }
            let contracts: Vec<Address> = serde_json::from_str(&contracts).unwrap_or_default();
            Ok(Some(contracts))
        });
        Ok(result.unwrap_or(None))
    }

    fn save_cached_contracts(&self, address: &str, contracts: &[Address]) -> SqliteResult<()> {
        let conn = self.scanned.lock().unwrap();
        let contracts_json = serde_json::to_string(contracts).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO etherscan_cache (address, contracts, cached_at) VALUES (?, ?, ?)",
            (&address, &contracts_json, Local::now().to_rfc3339()),
        )?;
        info!("Cached {} contracts for {}", contracts.len(), address);
        Ok(())
    }

    fn cleanup_old_scanned_contracts(&self) -> SqliteResult<()> {
        let conn = self.scanned.lock().unwrap();
        let cutoff = Local::now()
            .checked_sub_signed(chrono::Duration::days(7))
            .unwrap()
            .to_rfc3339();
        let deleted = conn.execute(
            "DELETE FROM scanned_contracts WHERE processed = 1 AND scanned_at < ?",
            [&cutoff],
        )?;
        info!("Deleted {} old scanned contracts", deleted);
        Ok(())
    }
}

// SSH Connection Pool
#[derive(Clone)]
struct SSHPool {
    clients: Arc<Mutex<Vec<(String, String, Option<Session>)>>>,
    installed_hosts: Arc<Mutex<HashSet<String>>>,
    active_tasks: Arc<Mutex<HashMap<String, usize>>>,
}

impl SSHPool {
    async fn new(hosts: &[(String, String)], user: &str) -> Result<Self> {
        let mut clients = Vec::new();
        for (host, password) in hosts {
            let client = match Self::connect_host(host, user, password).await {
                Ok(client) => {
                    info!("Successfully connected to SSH host {}", host);
                    Some(client)
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", host, e);
                    None
                }
            };
            clients.push((host.clone(), password.clone(), client));
        }
        Ok(SSHPool {
            clients: Arc::new(Mutex::new(clients)),
            installed_hosts: Arc::new(Mutex::new(HashSet::new())),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn connect_host(host: &str, user: &str, password: &str) -> Result<Session> {
        let mut attempts = 0;
        while attempts < 3 {
            match TcpStream::connect(format!("{}:22", host)) {
                Ok(tcp) => {
                    let mut sess = Session::new()?;
                    sess.set_tcp_stream(tcp);
                    sess.handshake()?;
                    sess.userauth_password(user, password)?;
                    return Ok(sess);
                }
                Err(e) => {
                    warn!("Connection attempt {} failed for {}: {}", attempts + 1, host, e);
                    attempts += 1;
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
        Err(anyhow::anyhow!("Failed to connect to {} after 3 attempts", host))
    }

    async fn execute(&self, host_idx: usize, command: &str) -> Result<(String, Option<u32>)> {
        let (host, password, client) = {
            let clients = self.clients.lock().unwrap();
            let idx = host_idx % clients.len();
            (
                clients[idx].0.clone(),
                clients[idx].1.clone(),
                clients[idx].2.clone(),
            )
        };

        let client = match client {
            Some(c) => c,
            None => {
                let new_client =
                    Self::connect_host(&host, &env::var("SSH_USER").unwrap(), &password).await?;
                let mut clients = self.clients.lock().unwrap();
                let idx = host_idx % clients.len();
                clients[idx].2 = Some(new_client.clone());
                new_client
            }
        };

        let wrapped_command = format!("{} & echo $!", command);
        let mut channel = client.channel_session()?;
        channel.exec(&wrapped_command)?;
        let mut output = Vec::new();
        channel.read_to_end(&mut output)?;
        channel.wait_close()?;
        if channel.exit_status()? != 0 {
            let new_client =
                Self::connect_host(&host, &env::var("SSH_USER").unwrap(), &password).await?;
            let mut clients = self.clients.lock().unwrap();
            let idx = host_idx % clients.len();
            clients[idx].2 = Some(new_client.clone());
            return Err(anyhow::anyhow!(
                "Command failed on {} with status {}",
                host,
                channel.exit_status()?
            ));
        }

        let output_str = String::from_utf8_lossy(&output).to_string();
        let pid = output_str
            .lines()
            .last()
            .and_then(|line| line.trim().parse::<u32>().ok());
        info!("Started process on {} with PID: {:?}", host, pid);

        {
            let mut active_tasks = self.active_tasks.lock().unwrap();
            *active_tasks.entry(host.clone()).or_insert(0) += 1;
        }

        Ok((output_str, pid))
    }

    async fn kill_process(&self, host_idx: usize, pid: u32) -> Result<()> {
        let host = {
            let clients = self.clients.lock().unwrap();
            let idx = host_idx % clients.len();
            clients[idx].0.clone()
        };
        let kill_cmd = format!("kill -9 {}", pid);
        self.execute(host_idx, &kill_cmd).await?;
        info!("Terminated process {} on host {}", pid, host);

        {
            let mut active_tasks = self.active_tasks.lock().unwrap();
            if let Some(count) = active_tasks.get_mut(&host) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    active_tasks.remove(&host);
                }
            }
        }

        Ok(())
    }

    async fn execute_with_stream(&self, host_idx: usize, command: &str) 
        -> Result<(ssh2::Channel, Option<u32>)> {
        let (host, password, client) = {
            let clients = self.clients.lock().unwrap();
            let idx = host_idx % clients.len();
            (
                clients[idx].0.clone(),
                clients[idx].1.clone(),
                clients[idx].2.clone(),
            )
        };

        let client = match client {
            Some(c) => c,
            None => {
                let new_client = Self::connect_host(&host, &env::var("SSH_USER").unwrap(), &password).await?;
                let mut clients = self.clients.lock().unwrap();
                let idx = host_idx % clients.len();
                clients[idx].2 = Some(new_client.clone());
                new_client
            }
        };

        // Create a channel and execute the command directly
        let mut channel = client.channel_session()?;
        
        // Execute the command and capture its output directly
        channel.exec(command)?;
        
        // Get the process ID using a separate channel
        let mut pid_channel = client.channel_session()?;
        pid_channel.exec("echo $$")?;
        let mut pid_output = Vec::new();
        pid_channel.read_to_end(&mut pid_output)?;
        let pid = String::from_utf8_lossy(&pid_output)
            .trim()
            .parse::<u32>()
            .ok();

        Ok((channel, pid))
    }

    async fn health_check(&self, user: &str) -> Result<()> {
        let hosts: Vec<(usize, String, String)> = {
            let clients = self.clients.lock().unwrap();
            clients
                .iter()
                .enumerate()
                .map(|(i, (host, password, _))| (i, host.clone(), password.clone()))
                .collect()
        };

        let mut join_set = JoinSet::new();
        for (idx, host, password) in hosts {
            let user_clone = user.to_string();
            let clients = Arc::clone(&self.clients);
            let installed_hosts = Arc::clone(&self.installed_hosts);
            join_set.spawn(async move {
                if installed_hosts.lock().unwrap().contains(&host) {
                    return Ok(());
                }

                let mut client = {
                    let clients = clients.lock().unwrap();
                    clients[idx].2.clone()
                };

                if client.is_none() {
                    match Self::connect_host(&host, &user_clone, &password).await {
                        Ok(new_client) => {
                            info!("Connected to {}", host);
                            let mut clients = clients.lock().unwrap();
                            clients[idx].2 = Some(new_client.clone());
                            client = Some(new_client);
                        }
                        Err(e) => {
                            warn!("Failed to connect to {}: {}", host, e);
                            return Err(e);
                        }
                    }
                }

                let sess = client.unwrap();
                let mut channel = sess.channel_session()?;
                channel.exec("echo test")?;
                let mut output = Vec::new();
                channel.read_to_end(&mut output)?;
                channel.wait_close()?;
                if channel.exit_status()? != 0 || String::from_utf8_lossy(&output).trim() != "test" {
                    let mut clients = clients.lock().unwrap();
                    clients[idx].2 = None;
                    return Err(anyhow::anyhow!("Connection test failed for {}", host));
                }

                let check_cmd = format!("{} --version", ITYFUZZ_PATH);
                let mut channel = sess.channel_session()?;
                channel.exec(&check_cmd)?;
                let mut output = Vec::new();
                channel.read_to_end(&mut output)?;
                channel.wait_close()?;
                if channel.exit_status()? == 0 {
                    info!("ItyFuzz already installed on {}", host);
                    installed_hosts.lock().unwrap().insert(host.clone());
                    return Ok(());
                }

                let mut install_attempts = 0;
                while install_attempts < 3 {
                    info!(
                        "Installing ItyFuzz on {} (attempt {}/{})",
                        host,
                        install_attempts + 1,
                        3
                    );
                    // Run installation script, source .bashrc, and execute ityfuzzup
                    let install_cmd = format!(
                        "{} && source /root/.bashrc && ityfuzzup",
                        ITYFUZZ_INSTALL_SCRIPT
                    );
                    let mut channel = sess.channel_session()?;
                    channel.exec(&install_cmd)?;
                    let mut install_output = Vec::new();
                    channel.read_to_end(&mut install_output)?;
                    channel.wait_close()?;
                    let install_status = channel.exit_status()?;
                    let install_output_str = String::from_utf8_lossy(&install_output).to_string();
                    if install_status != 0 {
                        warn!(
                            "ItyFuzz installation failed on {} (attempt {}): status {}, output: {}",
                            host,
                            install_attempts + 1,
                            install_status,
                            install_output_str
                        );
                        install_attempts += 1;
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }

                    // Verify installation
                    let mut verify_channel = sess.channel_session()?;
                    verify_channel.exec(&check_cmd)?;
                    let mut verify_output = Vec::new();
                    verify_channel.read_to_end(&mut verify_output)?;
                    verify_channel.wait_close()?;
                    let verify_status = verify_channel.exit_status()?;
                    let verify_output_str = String::from_utf8_lossy(&verify_output).to_string();
                    if verify_status == 0 {
                        info!("ItyFuzz installed and verified on {}", host);
                        installed_hosts.lock().unwrap().insert(host.clone());
                        return Ok(());
                    } else {
                        warn!(
                            "ItyFuzz verification failed on {} (attempt {}): status {}, output: {}",
                            host,
                            install_attempts + 1,
                            verify_status,
                            verify_output_str
                        );
                        install_attempts += 1;
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                }

                Err(anyhow::anyhow!(
                    "ItyFuzz installation failed on {} after 3 attempts",
                    host
                ))
            });
        }

        while let Some(result) = join_set.join_next().await {
            result??;
        }

        Ok(())
    }

    async fn periodic_health_check(&self, user: &str) {
        loop {
            sleep(SSH_HEALTH_CHECK_INTERVAL).await;
            if let Err(e) = self.health_check(user).await {
                warn!("Periodic health check failed: {}", e);
            }
            // Reset task counts for hosts with no active sessions
            let mut active_tasks = self.active_tasks.lock().unwrap();
            active_tasks.retain(|host, count| *count > 0);
            info!("Active tasks after health check: {:?}", active_tasks);
        }
    }

    fn get_available_host(&self) -> Option<(usize, String)> {
        let clients = self.clients.lock().unwrap();
        let active_tasks = self.active_tasks.lock().unwrap();
        let available = clients
            .iter()
            .enumerate()
            .filter(|(_, (host, _, session))| {
                session.is_some() && active_tasks.get(host).copied().unwrap_or(0) < TASKS_PER_HOST
            })
            .collect::<Vec<_>>();
        if available.is_empty() {
            return None;
        }
        // Round-robin selection
        static mut COUNTER: usize = 0;
        unsafe {
            let idx = COUNTER % available.len();
            COUNTER += 1;
            Some((available[idx].0, available[idx].1.0.clone()))
        }
    }
}

// Hot Wallet Tracker
struct HotWalletTracker {
    transactions: HashMap<Address, Vec<Instant>>,
    alerted: HashMap<Address, Instant>,
    threshold: u32,
    window: u64,
    alert_cooldown: u64,
}

impl HotWalletTracker {
    fn new(threshold: u32, window: u64) -> Self {
        HotWalletTracker {
            transactions: HashMap::new(),
            alerted: HashMap::new(),
            threshold,
            window,
            alert_cooldown: 300,
        }
    }

    fn track(&mut self, address: Address) -> bool {
        let now = Instant::now();

        if let Some(last_alert) = self.alerted.get(&address) {
            if now.duration_since(*last_alert).as_secs() < self.alert_cooldown {
                return false;
            }
        }

        let entry = self.transactions.entry(address).or_insert_with(Vec::new);
        entry.push(now);
        entry.retain(|&t| now.duration_since(t).as_secs() <= self.window);
        let is_hot = entry.len() >= self.threshold as usize;
        if is_hot {
            self.alerted.insert(address, now);
            self.alerted
                .retain(|_, t| now.duration_since(*t).as_secs() <= self.alert_cooldown);
        }
        is_hot
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_file_path = env::var("LOG_FILE").unwrap_or("vanity_logs.txt".to_string());
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)?;
    let log_file = Arc::new(Mutex::new(log_file));
    let tee_writer = TeeWriter {
        file: Arc::clone(&log_file),
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
            writeln!(
                buf,
                "[{}] {}: {}",
                timestamp,
                record.level(),
                record.args()
            )
        })
        .target(env_logger::Target::Pipe(Box::new(tee_writer)))
        .init();

    info!("Starting mempool fuzzer");

    dotenv().ok();
    let config = Config {
        infura_api_key: env::var("INFURA_API_KEY").expect("INFURA_API_KEY missing"),
        ssh_hosts: parse_ssh_hosts(&env::var("SSH_HOSTS").expect("SSH_HOSTS missing")),
        ssh_user: env::var("SSH_USER").expect("SSH_USER missing"),
        hot_wallet_threshold: env::var("HOT_WALLET_THRESHOLD")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .expect("Invalid HOT_WALLET_THRESHOLD"),
        hot_wallet_window: env::var("HOT_WALLET_WINDOW")
            .unwrap_or_else(|_| "60".to_string())
            .parse()
            .expect("Invalid HOT_WALLET_WINDOW"),
        wallet_private_key: env::var("WALLET_PRIVATE_KEY").expect("WALLET_PRIVATE_KEY missing"),
        log_file: log_file_path,
        database_file: env::var("DATABASE_FILE").unwrap_or("vanity.db".to_string()),
        telegram_bot_token: "6926474815:AAFx9tLAnf5OAVQZp2teS3G2_6T1wCP67xM".to_string(),
        telegram_chat_id: TELEGRAM_CHAT_ID,
    };

    let db = Database::new(&config.database_file, "deployed_contracts.db")?;
    let ssh_pool = SSHPool::new(&config.ssh_hosts, &config.ssh_user).await?;
    let bot = Arc::new(Bot::new(&config.telegram_bot_token));
    let active_sessions: Arc<Mutex<Vec<SessionInfo>>> = Arc::new(Mutex::new(Vec::new()));
    let hot_wallet_tracker = Arc::new(Mutex::new(HotWalletTracker::new(
        config.hot_wallet_threshold,
        config.hot_wallet_window,
    )));
    let contract_cache = Arc::new(Mutex::new(LruCache::new(
        NonZeroUsize::new(CONTRACT_CACHE_SIZE).unwrap(),
    )));

    info!("Performing initial SSH health check");
    if let Err(e) = ssh_pool.health_check(&config.ssh_user).await {
        error!("Initial SSH health check failed: {}", e);
        let _ = send_telegram_alert(&bot, &format!("Initial SSH health check failed: {}", e), AlertType::Error).await;
        return Err(e);
    }

    info!("Testing Telegram connection");
    if let Err(e) = check_telegram_connection(&bot).await {
        warn!("Telegram alerts disabled due to connection failure: {}", e);
    }

    let shutdown = Arc::new(Mutex::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let active_sessions_clone = Arc::clone(&active_sessions);
    let ssh_pool_clone: Arc<SSHPool> = Arc::new(ssh_pool.clone());
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        info!("Received shutdown signal");
        *shutdown_clone.lock().unwrap() = true;

        // Get the sessions that need to be cleaned up
        let sessions_to_cleanup: Vec<(usize, u32, String)> = {
            let sessions = active_sessions_clone.lock().unwrap();
            sessions
                .iter()
                .filter_map(|session| {
                    session.process_id.map(|(host_idx, pid)| 
                        (host_idx, pid, session.address.clone())
                    )
                })
                .collect()
        };

        // Clean up each session
        for (host_idx, pid, address) in sessions_to_cleanup {
            if let Err(e) = ssh_pool_clone.kill_process(host_idx, pid).await {
                warn!(
                    "Failed to terminate process {} for {}: {}",
                    pid, address, e
                );
            }
        }

        // Clear the sessions after cleanup
        {
            let mut sessions = active_sessions_clone.lock().unwrap();
            sessions.clear();
        }

        info!("Cleaned up all active sessions and processes");
    });

    let config = Arc::new(config);
    let db = Arc::new(db);
    let ssh_pool = Arc::new(ssh_pool);
    let active_sessions_clone = Arc::clone(&active_sessions);
    let contract_cache = Arc::clone(&contract_cache);

    let ssh_pool_clone = Arc::clone(&ssh_pool);
    tokio::spawn(async move {
        check_and_kill_timeout_sessions(&active_sessions_clone, &ssh_pool_clone).await;
    });

    let ssh_pool_clone = Arc::clone(&ssh_pool);
    let ssh_user = config.ssh_user.clone();
    tokio::spawn(async move {
        ssh_pool_clone.periodic_health_check(&ssh_user).await;
    });

    let config_clone = Arc::clone(&config);
    let db_clone = Arc::clone(&db);
    let ssh_pool_clone = Arc::clone(&ssh_pool);
    let _bot_clone = Arc::clone(&bot);
    let active_sessions_clone = Arc::clone(&active_sessions);
    tokio::spawn(async move {
        process_pending_contracts(
            &config_clone,
            &db_clone,
            &ssh_pool_clone,
            &_bot_clone,
            &active_sessions_clone,
        )
        .await;
    });

    let config_clone = Arc::clone(&config);
    let db_clone = Arc::clone(&db);
    let ssh_pool_clone = Arc::clone(&ssh_pool);
    let _bot_clone = Arc::clone(&bot);
    let active_sessions_clone = Arc::clone(&active_sessions);
    let contract_cache_clone = Arc::clone(&contract_cache);
    tokio::spawn(async move {
        monitor_deployed_contracts(
            &config_clone,
            &db_clone,
            &ssh_pool_clone,
            &_bot_clone,
            &active_sessions_clone,
            &contract_cache_clone,
        )
        .await
        .unwrap();
    });

    let db_clone = Arc::clone(&db);
    tokio::spawn(async move {
        loop {
            sleep(DATABASE_CLEANUP_INTERVAL).await;
            if let Err(e) = db_clone.cleanup_old_scanned_contracts() {
                error!("Failed to clean up old scanned contracts: {}", e);
            }
        }
    });

    let mut last_activity = Instant::now();
    while !*shutdown.lock().unwrap() {
        if last_activity.elapsed() > NO_ACTIVITY_TIMEOUT {
            warn!("No activity for 30 minutes, restarting scan");
            last_activity = Instant::now();
        }

        match scan_mempool(
            &config,
            &db,
            &ssh_pool,
            &bot,
            &active_sessions,
            &hot_wallet_tracker,
            &contract_cache,
        )
        .await
        {
            Ok(_) => last_activity = Instant::now(),
            Err(e) => {
                error!("Mempool scan error: {}", e);
                let _ = send_telegram_alert(&bot, &format!("Mempool scan error: {}", e), AlertType::Error).await;
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    info!("Shutting down gracefully");
    Ok(())
}

fn parse_ssh_hosts(ssh_hosts: &str) -> Vec<(String, String)> {
    ssh_hosts
        .split(',')
        .filter_map(|host| {
            let parts: Vec<&str> = host.split(':').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

async fn scan_mempool(
    config: &Arc<Config>,
    db: &Arc<Database>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    hot_wallet_tracker: &Arc<Mutex<HotWalletTracker>>,
    contract_cache: &Arc<Mutex<LruCache<Address, bool>>>,
) -> Result<()> {
    let ws_url = format!(
        "ws://127.0.0.1:8546"
    );
    let provider = Arc::new(Provider::<Ws>::connect(&ws_url).await?);
    let mut subscription = provider.subscribe_pending_txs().await?;

    info!("Monitoring Ethereum mempool");

    let mut batch = Vec::new();
    let mut last_process_time = Instant::now();
    const BATCH_SIZE: usize = 10;
    const MIN_PROCESS_INTERVAL: Duration = Duration::from_secs(5); // Minimum time between processing batches

    while let Some(tx_hash) = subscription.next().await {
        batch.push(tx_hash);

        // Check if we should process the current batch
        let should_process = batch.len() >= BATCH_SIZE || last_process_time.elapsed() >= MIN_PROCESS_INTERVAL;
        
        if should_process {
            // Check if there are active sessions
            let active_session_count = active_sessions.lock().unwrap().len();
            
            if active_session_count >= MAX_CONCURRENCY {
                // If we're at max capacity, wait longer
                info!("At max capacity ({} active sessions), waiting...", active_session_count);
                sleep(Duration::from_secs(30)).await;
                continue;
            }

            // Process the batch if we have space
            let provider = Arc::clone(&provider);
            let config = Arc::clone(config);
            let db = Arc::clone(db);
            let ssh_pool = Arc::clone(ssh_pool);
            let bot = Arc::clone(bot);
            let active_sessions = Arc::clone(active_sessions);
            let hot_wallet_tracker = Arc::clone(hot_wallet_tracker);
            let contract_cache = Arc::clone(contract_cache);
            let current_batch = std::mem::take(&mut batch);
            
            task::spawn(async move {
                for tx_hash in current_batch {
                    if let Err(e) = process_transaction(
                        &provider,
                        &config,
                        &db,
                        &ssh_pool,
                        &bot,
                        &active_sessions,
                        &hot_wallet_tracker,
                        &contract_cache,
                        tx_hash,
                    )
                    .await
                    {
                        error!("Error processing tx {}: {}", tx_hash, e);
                    }
                    
                    // Add a small delay between transactions to reduce API load
                    sleep(Duration::from_millis(200)).await;
                }
            });

            last_process_time = Instant::now();
        } else {
            // If we're not processing yet, add a small delay
            sleep(Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

async fn process_transaction(
    provider: &Arc<Provider<Ws>>,
    config: &Arc<Config>,
    db: &Arc<Database>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    hot_wallet_tracker: &Arc<Mutex<HotWalletTracker>>,
    contract_cache: &Arc<Mutex<LruCache<Address, bool>>>,
    tx_hash: H256,
) -> Result<()> {
    let tx = retry(ExponentialBackoff::default(), || async {
        let tx = provider
            .get_transaction(tx_hash)
            .await
            .map_err(|e| BackoffError::transient(anyhow::anyhow!(e)))?;
        match tx {
            Some(tx) => Ok(tx),
            None => Err(BackoffError::transient(anyhow::anyhow!(
                "Transaction {} not found",
                tx_hash
            ))),
        }
    })
    .await?;

    let from = tx.from;
    let to = tx.to;

    let is_hot_wallet = {
        let mut tracker = hot_wallet_tracker.lock().unwrap();
        tracker.track(from)
    };
    if is_hot_wallet {
        info!("Hot wallet detected: {}", from);
        let _ = send_telegram_alert(
            bot,
            &format!("Hot Wallet detected: {}", from),
            AlertType::Info,
        )
        .await;
    }

    if to.is_none() {
        if let Some(receipt) = retry(ExponentialBackoff::default(), || async {
            Ok(provider.get_transaction_receipt(tx_hash).await?)
        })
        .await?
        {
            if let Some(contract_addr) = receipt.contract_address {
                let addr_str = format!("0x{}", hex::encode(contract_addr.as_bytes()));
                info!("Detected contract deployment: {}", addr_str);
                db.save_deployed_contract(&addr_str)?;
                let target_contract = contract_addr;
                let target_addr_str = addr_str.clone();
                if !db.is_scanned(&target_addr_str)? {
                    process_contract(
                        provider,
                        config,
                        db,
                        ssh_pool,
                        bot,
                        active_sessions,
                        contract_cache,
                        target_contract,
                        &target_addr_str,
                        true,
                        is_hot_wallet,
                        &tx,
                    )
                    .await?;
                }
                return Ok(());
            }
        }
    }

    let target_contract = match to {
        Some(addr) if is_contract(provider, addr, contract_cache).await? => addr,
        _ => {
            info!("Transaction {} does not involve a contract", tx_hash);
            return Ok(());
        }
    };

    let target_addr_str = format!("0x{}", hex::encode(target_contract.as_bytes()));
    if db.is_scanned(&target_addr_str)? {
        info!("Skipping fully processed contract: {}", target_addr_str);
        return Ok(());
    }

    process_contract(
        provider,
        config,
        db,
        ssh_pool,
        bot,
        active_sessions,
        contract_cache,
        target_contract,
        &target_addr_str,
        false,
        is_hot_wallet,
        &tx,
    )
    .await?;

    Ok(())
}

async fn process_contract(
    provider: &Arc<Provider<Ws>>,
    _config: &Arc<Config>,
    db: &Arc<Database>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    contract_cache: &Arc<Mutex<LruCache<Address, bool>>>,
    target_contract: Address,
    target_addr_str: &str,
    is_deployed: bool,
    is_hot_wallet: bool,
    tx: &Transaction,
) -> Result<()> {
    info!(
        "Processing contract: {} (Hot wallet: {}, Deployed: {})",
        target_addr_str, is_hot_wallet, is_deployed
    );

    let mut contracts = HashSet::new();
    contracts.insert(target_contract);

    let tokens = extract_tokens(tx, provider, contract_cache).await?;
    for token in tokens {
        if is_contract(provider, token, contract_cache).await? {
            contracts.insert(token);
        }
    }

    let interacted_contracts =
        get_interacted_contracts(provider, _config, db, target_contract, contract_cache).await?;
    for contract in interacted_contracts {
        if is_contract(provider, contract, contract_cache).await? {
            contracts.insert(contract);
        }
    }

    let mut addresses: Vec<String> = contracts
        .into_iter()
        .map(|addr| format!("0x{}", hex::encode(addr.as_bytes())))
        .collect();
    if addresses.len() <= 1 {
        for stablecoin in STABLECOINS {
            let decoded = hex::decode(stablecoin.strip_prefix("0x").unwrap_or(stablecoin))?;
            let addr = ethers::types::H160::from_slice(&decoded);
            if is_contract(provider, addr, contract_cache).await? {
                addresses.push(stablecoin.to_string());
            }
        }
    }
    let addresses_str = addresses.join(",");

    let processed = {
        let active_sessions_count = active_sessions.lock().unwrap().len();
        let has_slot = active_sessions_count < MAX_CONCURRENCY;
        let is_priority = is_hot_wallet || is_deployed;
        has_slot || (is_priority && active_sessions_count < MAX_CONCURRENCY + 2)
    };

    db.save_contract(target_addr_str, &addresses_str, processed)?;

    if processed {
        info!("Scheduling fuzzing for contract: {}", target_addr_str);
        let db_clone = Arc::clone(db);
        let config_clone = Arc::clone(_config);
        let ssh_pool_clone = Arc::clone(ssh_pool);
        let bot_clone = Arc::clone(bot);
        let active_sessions_clone = Arc::clone(active_sessions);
        let target_addr_str_clone = target_addr_str.to_string();
        task::spawn(async move {
            let start = Instant::now();
            let mut project_retry_count = 0;
            let result = run_fuzzer(
                &addresses_str,
                &config_clone,
                &ssh_pool_clone,
                &bot_clone,
                &active_sessions_clone,
                "mempool_project",
                &mut project_retry_count,
                "eth",
            )
            .await;
            match result {
                Ok(is_success) => {
                    if is_success {
                        info!(
                            "Vulnerability found for contracts: {} in {:?}",
                            addresses_str,
                            start.elapsed()
                        );
                    } else {
                        info!(
                            "No vulnerabilities found for contracts: {} in {:?}",
                            addresses_str,
                            start.elapsed()
                        );
                        let _ = send_telegram_alert(
                            &bot_clone,
                            &format!(
                                "Completed fuzzing for {}\nNo vulnerabilities found",
                                target_addr_str_clone
                            ),
                            AlertType::Info,
                        )
                        .await;
                    }
                    if let Err(e) = db_clone.mark_processed(&target_addr_str_clone) {
                        error!("Failed to mark {} as processed: {}", target_addr_str_clone, e);
                    }
                }
                Err(e) => {
                    error!("Fuzzing failed for {}: {}", target_addr_str_clone, e);
                    let _ = send_telegram_alert(
                        &bot_clone,
                        &format!("Fuzzing failed for {}: {}", target_addr_str_clone, e),
                        AlertType::Error,
                    )
                    .await;
                }
            }
        });
    } else {
        info!("SSH hosts busy, queued contract: {}", target_addr_str);
    }

    Ok(())
}

async fn process_pending_contracts(
    config: &Arc<Config>,
    db: &Arc<Database>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
) {
    loop {
        // Check active sessions first
        let active_sessions_count = active_sessions.lock().unwrap().len();
        
        if active_sessions_count >= MAX_CONCURRENCY {
            // If we're at capacity, wait longer before checking again
            info!("Max concurrent sessions reached ({}), waiting...", active_sessions_count);
            sleep(Duration::from_secs(60)).await;
            continue;
        }

        // Calculate available slots
        let available_slots = MAX_CONCURRENCY - active_sessions_count;

        // Only fetch pending contracts if we have slots available
        if available_slots > 0 {
            match db.get_pending_contracts() {
                Ok(pending) => {
                    if pending.is_empty() {
                        // If no pending contracts, wait longer
                        sleep(Duration::from_secs(30)).await;
                        continue;
                    }

                    for (address, addresses_str) in pending.into_iter().take(available_slots) {
                        info!("Processing pending contract: {}", address);
                        let db_clone = Arc::clone(db);
                        let config_clone = Arc::clone(config);
                        let ssh_pool_clone = Arc::clone(ssh_pool);
                        let bot_clone = Arc::clone(bot);
                        let active_sessions_clone = Arc::clone(active_sessions);
                        
                        task::spawn(async move {
                            let start = Instant::now();
                            let mut project_retry_count = 0;
                            let result = run_fuzzer(
                                &addresses_str,
                                &config_clone,
                                &ssh_pool_clone,
                                &bot_clone,
                                &active_sessions_clone,
                                "mempool_project",
                                &mut project_retry_count,
                                "eth",
                            )
                            .await;

                            match result {
                                Ok(is_success) => {
                                    if is_success {
                                        info!(
                                            "Vulnerability found for contracts: {} in {:?}",
                                            addresses_str,
                                            start.elapsed()
                                        );
                                    } else {
                                        info!(
                                            "No vulnerabilities found for contracts: {} in {:?}",
                                            addresses_str,
                                            start.elapsed()
                                        );
                                        let _ = send_telegram_alert(
                                            &bot_clone,
                                            &format!(
                                                "Completed fuzzing for {}\nNo vulnerabilities found",
                                                address
                                            ),
                                            AlertType::Info,
                                        )
                                        .await;
                                    }
                                    if let Err(e) = db_clone.mark_processed(&address) {
                                        error!("Failed to mark {} as processed: {}", address, e);
                                    }
                                }
                                Err(e) => {
                                    error!("Fuzzing failed for {}: {}", address, e);
                                    let _ = send_telegram_alert(
                                        &bot_clone,
                                        &format!("Fuzzing failed for {}: {}", address, e),
                                        AlertType::Error,
                                    )
                                    .await;
                                }
                            }
                        });

                        // Add delay between spawning tasks to prevent overwhelming the system
                        sleep(Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    error!("Failed to fetch pending contracts: {}", e);
                    sleep(Duration::from_secs(30)).await;
                }
            }
        }

        // Wait before next check
        sleep(PENDING_CHECK_INTERVAL).await;
    }
}

async fn monitor_deployed_contracts(
    _config: &Arc<Config>,
    db: &Arc<Database>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    contract_cache: &Arc<Mutex<LruCache<Address, bool>>>,
) -> Result<()> {
    let client = Arc::new(Client::new());
    let provider = Arc::new(Provider::<Ws>::connect(format!(
        "ws://127.0.0.1:8546"
    ))
    .await?);
    loop {
        sleep(DEPLOYED_CHECK_INTERVAL).await;
        let contracts = match db.get_deployed_contracts() {
            Ok(contracts) => contracts,
            Err(e) => {
                error!("Failed to fetch deployed contracts: {}", e);
                continue;
            }
        };

        for (address, last_checked, tx_count) in contracts {
            let last_checked_time = chrono::DateTime::parse_from_rfc3339(&last_checked)
                .map(|dt| dt.timestamp())
                .unwrap_or(0);
            if SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - last_checked_time
                < 24 * 3600
            {
                continue;
            }

            let url = format!(
                "https://api.etherscan.io/api?module=account&action=txlist&address={}&sort=asc&apikey={}",
                address, ETHERSCAN_API_KEY
            );

            let response = retry(ExponentialBackoff::default(), || async {
                sleep(API_RATE_LIMIT).await;
                Ok(client.get(&url).send().await?.json::<EtherscanResponse>().await?)
            })
            .await?;

            let new_tx_count = if response.status == "1" {
                response.result.len() as i32
            } else {
                tx_count
            };
            if let Err(e) = db.update_deployed_contract(&address, new_tx_count) {
                error!("Failed to update {}: {}", address, e);
                continue;
            }

            if new_tx_count > 2 && !db.is_scanned(&address)? {
                let mut contracts = HashSet::new();
                if response.status == "1" {
                    for tx in response.result {
                        let decoded = hex::decode(tx.from.strip_prefix("0x").unwrap_or(&tx.from))?;
                        let from = ethers::types::H160::from_slice(&decoded);
                        if is_contract(&provider, from, contract_cache).await? {
                            contracts.insert(from);
                        }
                        if !tx.to.is_empty() {
                            let decoded = hex::decode(tx.to.strip_prefix("0x").unwrap_or(&tx.to))?;
                            let to = ethers::types::H160::from_slice(&decoded);
                            if is_contract(&provider, to, contract_cache).await? {
                                contracts.insert(to);
                            }
                        }
                        if !tx.contractAddress.is_empty() {
                            let decoded = hex::decode(
                                tx.contractAddress
                                    .strip_prefix("0x")
                                    .unwrap_or(&tx.contractAddress),
                            )?;
                            let addr = ethers::types::H160::from_slice(&decoded);
                            if is_contract(&provider, addr, contract_cache).await? {
                                contracts.insert(addr);
                            }
                        }
                    }
                }

                let mut addresses: Vec<String> = contracts
                    .into_iter()
                    .map(|addr| format!("0x{}", hex::encode(addr.as_bytes())))
                    .collect();
                addresses.push(address.clone());
                for stablecoin in STABLECOINS {
                    let decoded =
                        hex::decode(stablecoin.strip_prefix("0x").unwrap_or(stablecoin))?;
                    let addr = ethers::types::H160::from_slice(&decoded);
                    if is_contract(&provider, addr, contract_cache).await? {
                        addresses.push(stablecoin.to_string());
                    }
                }
                let addresses_str = addresses.join(",");

                let processed = active_sessions.lock().unwrap().len() < MAX_CONCURRENCY;
                if let Err(e) = db.save_contract(&address, &addresses_str, processed) {
                    error!("Failed to save deployed contract {}: {}", address, e);
                    continue;
                }

                if processed {
                    info!("Scheduling fuzzing for deployed contract: {}", address);
                    let db_clone = Arc::clone(db);
                    let config_clone = Arc::clone(_config);
                    let ssh_pool_clone = Arc::clone(ssh_pool);
                    let bot_clone = Arc::clone(bot);
                    let active_sessions_clone = Arc::clone(active_sessions);
                    task::spawn(async move {
                        let start = Instant::now();
                        let mut project_retry_count = 0;
                        let result = run_fuzzer(
                            &addresses_str,
                            &config_clone,
                            &ssh_pool_clone,
                            &bot_clone,
                            &active_sessions_clone,
                            "mempool_project",
                            &mut project_retry_count,
                            "eth",
                        )
                        .await;
                        match result {
                            Ok(is_success) => {
                                if is_success {
                                    info!(
                                        "Vulnerability found for deployed contracts: {} in {:?}",
                                        addresses_str,
                                        start.elapsed()
                                    );
                                } else {
                                    info!(
                                        "No vulnerabilities found for deployed contracts: {} in \
                                         {:?}",
                                        addresses_str,
                                        start.elapsed()
                                    );
                                    let _ = send_telegram_alert(
                                        &bot_clone,
                                        &format!(
                                            "Completed fuzzing for {}\nNo vulnerabilities found",
                                            address
                                        ),
                                        AlertType::Info,
                                    )
                                    .await;
                                }
                                if let Err(e) = db_clone.mark_processed(&address) {
                                    error!("Failed to mark {} as processed: {}", address, e);
                                }
                            }
                            Err(e) => {
                                error!("Fuzzing failed for {}: {}", address, e);
                                let _ = send_telegram_alert(
                                    &bot_clone,
                                    &format!("Fuzzing failed for {}: {}", address, e),
                                    AlertType::Error,
                                )
                                .await;
                            }
                        }
                    });
                } else {
                    info!("SSH hosts busy, queued deployed contract: {}", address);
                }
            }
        }
    }
}

async fn is_contract(
    provider: &Arc<Provider<Ws>>,
    address: Address,
    cache: &Arc<Mutex<LruCache<Address, bool>>>,
) -> Result<bool> {
    {
        let mut cache = cache.lock().unwrap();
        if let Some(&is_contract) = cache.get(&address) {
            return Ok(is_contract);
        }
    }

    let code = retry(ExponentialBackoff::default(), || async {
        Ok(provider.get_code(address, None).await?)
    })
    .await?;
    let is_contract = !code.is_empty();

    let mut cache = cache.lock().unwrap();
    cache.put(address, is_contract);
    Ok(is_contract)
}

async fn extract_tokens(
    tx: &Transaction,
    provider: &Arc<Provider<Ws>>,
    cache: &Arc<Mutex<LruCache<Address, bool>>>,
) -> Result<Vec<Address>> {
    let mut tokens = HashSet::new();

    if !tx.input.is_empty() {
        let erc20_signatures = [
            [0xa9, 0x05, 0x9c, 0xbb], // transfer
            [0x23, 0xb8, 0x72, 0xdd], // transferFrom
            [0x09, 0x5e, 0xa7, 0xb3], // approve
            [0x40, 0xc1, 0x0f, 0x19], // mint
            [0x42, 0x96, 0x6c, 0x68], // burn
        ];
        if tx.input.len() >= 4 && erc20_signatures.iter().any(|sig| tx.input[0..4] == *sig) {
            if let Some(to) = tx.to {
                if is_contract(provider, to, cache).await? {
                    tokens.insert(to);
                }
            }
        }
    }

    if let Some(receipt) = retry(ExponentialBackoff::default(), || async {
        Ok(provider.get_transaction_receipt(tx.hash).await?)
    })
    .await?
    {
        for log in receipt.logs {
            if log.topics.len() > 0
                && log.topics[0]
                    == H256::from_slice(
                        &decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")?,
                    )
            {
                if is_contract(provider, log.address, cache).await? {
                    tokens.insert(log.address);
                }
            }
        }
    }

    Ok(tokens.into_iter().collect())
}

#[derive(Debug, Serialize, Deserialize)]
struct EtherscanTx {
    from: String,
    to: String,
    contractAddress: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EtherscanResponse {
    status: String,
    message: String,
    result: Vec<EtherscanTx>,
}

async fn get_interacted_contracts(
    provider: &Arc<Provider<Ws>>,
    _config: &Arc<Config>,
    db: &Arc<Database>,
    contract_addr: Address,
    cache: &Arc<Mutex<LruCache<Address, bool>>>,
) -> Result<Vec<Address>> {
    let addr_str = format!("0x{}", hex::encode(contract_addr.as_bytes()));

    if let Some(contracts) = db.get_cached_contracts(&addr_str)? {
        return Ok(contracts);
    }

    let client = Client::new();
    let url = format!(
        "https://api.etherscan.io/api?module=account&action=txlist&address={}&sort=asc&apikey={}",
        addr_str, ETHERSCAN_API_KEY
    );

    let response = retry(ExponentialBackoff::default(), || async {
        sleep(API_RATE_LIMIT).await;
        Ok(client.get(&url).send().await?.json::<EtherscanResponse>().await?)
    })
    .await?;

    let mut contracts = HashSet::new();
    if response.status == "1" {
        for tx in response.result {
            let decoded = hex::decode(tx.from.strip_prefix("0x").unwrap_or(&tx.from))?;
            let from = ethers::types::H160::from_slice(&decoded);
            if is_contract(provider, from, cache).await? {
                contracts.insert(from);
            }
            if !tx.to.is_empty() {
                let decoded = hex::decode(tx.to.strip_prefix("0x").unwrap_or(&tx.to))?;
                let to = ethers::types::H160::from_slice(&decoded);
                if is_contract(provider, to, cache).await? {
                    contracts.insert(to);
                }
            }
            if !tx.contractAddress.is_empty() {
                let decoded = hex::decode(
                    tx.contractAddress
                        .strip_prefix("0x")
                        .unwrap_or(&tx.contractAddress),
                )?;
                let addr = ethers::types::H160::from_slice(&decoded);
                if is_contract(provider, addr, cache).await? {
                    contracts.insert(addr);
                }
            }
        }
    }

    let contracts_vec: Vec<Address> = contracts.into_iter().collect();
    db.save_cached_contracts(&addr_str, &contracts_vec)?;
    Ok(contracts_vec)
}

async fn run_fuzzer(
    addresses: &str,
    _config: &Arc<Config>,
    ssh_pool: &Arc<SSHPool>,
    bot: &Arc<Bot>,
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    project_name: &str,
    project_retry_count: &mut usize,
    _blockchain_type: &str,
) -> Result<bool> {
    if *project_retry_count >= 3 {
        error!(
            "Project {} exceeded max retries for {}",
            project_name, addresses
        );
        let _ = send_telegram_alert(
            bot,
            &format!("Project {} exceeded max retries for {}", project_name, addresses),
            AlertType::Error,
        )
        .await;
        return Ok(false);
    }

    let log_path = get_log_path(addresses, project_name);
    fs::create_dir_all(Path::new(&log_path).parent().unwrap())?;
    let _log_file = File::create(&log_path)?;

    let command_str = format!(
        "set ETH_RPC_URL=ws://127.0.0.1:8546 &&
        ityfuzz evm -t {} --onchain-block-number 22361664 -c ETH --flashloan --onchain-etherscan-api-key {}",
        addresses, 
        ETHERSCAN_API_KEY
    );
    info!("Preparing to fuzz contracts: {}", addresses);

    let mut join_set = JoinSet::new();
    let mut session_info = SessionInfo {
        start_time: Instant::now(),
        address: addresses.to_string(),
        process_id: None,
        verified: false,
        last_check: Instant::now(),
        project_name: project_name.to_string(),
        found_vulnerability: false,
        process_complete: false,
        last_log_update: Instant::now(),
        vulnerability_details: None,
    };

    let host_info = ssh_pool.get_available_host();
    match host_info {
        Some((host_idx, host)) => {
            info!("Assigning fuzzing task to host {} for {}", host, addresses);
            let command_clone = command_str.clone();
            let addresses_clone = addresses.to_string();
            let ssh_pool_clone = Arc::clone(ssh_pool);
            let _bot_clone: Arc<Bot> = bot.clone();
            let retry_count = *project_retry_count;
            join_set.spawn(async move {
                info!(
                    "Starting ItyFuzz on {} for {} (attempt {}/{})",
                    host,
                    addresses_clone,
                    retry_count + 1,
                    3
                );
                let result = timeout(ITYFUZZ_TIMEOUT, async {
                    retry(
                        ExponentialBackoff {
                            max_elapsed_time: Some(Duration::from_secs(1800)),
                            ..Default::default()
                        },
                        || async {
                            let (mut channel, pid) = ssh_pool_clone.execute_with_stream(host_idx, &command_clone).await?;
                            let mut output = String::new();
                            let mut buffer = [0; 1024];
                            
                            // Create a channel for communication between the blocking read thread and async context
                            let (tx, mut rx) = mpsc::channel(32);

                            // Spawn a blocking task to read from the SSH channel
                            let read_handle = tokio::task::spawn_blocking(move || {
                                info!("Starting to read from SSH channel");
                                let mut last_output_time = Instant::now();
                                
                                loop {
                                    match channel.read(&mut buffer) {
                                        Ok(n) => {
                                            if n == 0 {
                                                // Only break if we haven't received output for a while
                                                if last_output_time.elapsed() > Duration::from_secs(5) {
                                                    info!("No output received for 5 seconds, assuming EOF");
                                                    break;
                                                }
                                                std::thread::sleep(Duration::from_millis(100));
                                                continue;
                                            }
                                            
                                            last_output_time = Instant::now();
                                            info!("Read {} bytes from SSH channel", n);
                                            
                                            if tx.blocking_send(Ok(buffer[..n].to_vec())).is_err() {
                                                info!("Channel send failed, breaking");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Error reading from SSH channel: {}", e);
                                            let _ = tx.blocking_send(Err(e));
                                            break;
                                        }
                                    }
                                }
                                info!("SSH channel read loop completed");
                            });

                            // Create and pin the timeout future
                            let sleep = tokio::time::sleep(Duration::from_secs(1800));
                            tokio::pin!(sleep);

                            // Process the output in the async context
                            let mut last_output_time = Instant::now();
                            loop {
                                tokio::select! {
                                    maybe_result = rx.recv() => {
                                        match maybe_result {
                                            Some(Ok(data)) => {
                                                last_output_time = Instant::now();
                                                let new_data = String::from_utf8_lossy(&data);
                                                info!("Received {} bytes of new data: {}", data.len(), new_data);
                                                output.push_str(&new_data);
                                                if output.contains("vulnerabilities") || output.contains("Found") {
                                                    info!("Vulnerability found, stopping process");
                                                    read_handle.abort();
                                                    return Ok((output, pid));
                                                }
                                            }
                                            Some(Err(e)) => {
                                                warn!("Channel error: {}", e);
                                                read_handle.abort();
                                                return Err(BackoffError::permanent(anyhow::anyhow!(e)));
                                            }
                                            None => {
                                                if last_output_time.elapsed() > Duration::from_secs(30) {
                                                    info!("No output received for 30 seconds, stopping process");
                                                    read_handle.abort();
                                                    return Ok((output, pid));
                                                }
                                                tokio::time::sleep(Duration::from_secs(1)).await;
                                                continue;
                                            }
                                        }
                                    }
                                    _ = &mut sleep => {
                                        info!("Timeout reached after 30 minutes");
                                        read_handle.abort();
                                        return Ok((output, pid));
                                    }
                                }
                            }
                        },
                    )
                    .await
                })
                .await;

                match result {
                    Ok(Ok((output, pid))) => {
                        info!(
                            "ItyFuzz completed on {} for {}. Output length: {}, PID: {:?}",
                            host,
                            addresses_clone,
                            output.len(),
                            pid
                        );
                        if output.trim().is_empty() {
                            warn!("ItyFuzz produced empty output on {} for {}", host, addresses_clone);
                        }
                        Ok((host, output, pid, host_idx))
                    }
                    Ok(Err(e)) => {
                        warn!("ItyFuzz failed on {} for {}: {}", host, addresses_clone, e);
                        Err(anyhow::anyhow!("ItyFuzz failed on {}: {}", host, e))
                    }
                    Err(_) => {
                        warn!("ItyFuzz timed out on {} for {}", host, addresses_clone);
                        Err(anyhow::anyhow!("ItyFuzz timed out on {}", host))
                    }
                }
            });
        }
        None => {
            error!("No available SSH hosts for fuzzing {}", addresses);
            let _ = send_telegram_alert(
                bot,
                &format!("No available SSH hosts for fuzzing {}", addresses),
                AlertType::Error,
            )
            .await;
            return Err(anyhow::anyhow!("No available SSH hosts"));
        }
    }

    let mut found_vulnerability = false;
    while let Some(result) = join_set.join_next().await {
        match result? {
            Ok((host, output, pid, host_idx)) => {
                info!("Processing ItyFuzz output from {} for {}", host, addresses);
                let mut file = OpenOptions::new().append(true).open(&log_path)?;
                file.write_all(output.as_bytes())?;

                session_info.process_id = pid.map(|p| (host_idx, p));

                if check_output_for_vulnerabilities(&output, &mut session_info, bot, addresses).await?
                {
                    found_vulnerability = true;
                    info!("Vulnerability found on {} for {}", host, addresses);
                } else {
                    info!("No vulnerabilities found in output from {} for {}", host, addresses);
                }

                {
                    let mut active_tasks = ssh_pool.active_tasks.lock().unwrap();
                    if let Some(count) = active_tasks.get_mut(&host) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            active_tasks.remove(&host);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Fuzzing task failed for {}: {}", addresses, e);
            }
        }
    }

    session_info.process_complete = true;
    if found_vulnerability {
        session_info.found_vulnerability = true;
    }
    active_sessions.lock().unwrap().push(session_info);

    if found_vulnerability {
        *project_retry_count += 1;
        info!("Fuzzing completed for {} with vulnerabilities", addresses);
        return Ok(true);
    }

    *project_retry_count += 1;
    info!("Fuzzing completed for {} with no vulnerabilities", addresses);
    Ok(false)
}

fn get_log_path(addr: &str, project_name: &str) -> String {
    let first_addr = addr.split(',').next().unwrap_or(addr);
    let short_addr = if first_addr.len() > 10 {
        &first_addr[0..10]
    } else {
        first_addr
    };
    format!("{}/{}/{}_eth.txt", PROJECT_DIR, project_name, short_addr)
}

fn get_vulnerability_log_path(project_name: &str) -> String {
    format!(
        "{}/{}/{}_vulnerabilities.txt",
        PROJECT_DIR, project_name, project_name
    )
}

async fn check_and_kill_timeout_sessions(
    active_sessions: &Arc<Mutex<Vec<SessionInfo>>>,
    ssh_pool: &Arc<SSHPool>,
) {
    loop {
        sleep(Duration::from_secs(30)).await;
        let mut sessions = active_sessions.lock().unwrap();
        let before = sessions.len();
        sessions.retain(|session| {
            if session.start_time.elapsed() >= ITYFUZZ_TIMEOUT || session.process_complete {
                if let Some((host_idx, pid)) = session.process_id {
                    let address = session.address.clone();
                    let ssh_pool_clone = Arc::clone(ssh_pool);
                    task::spawn(async move {
                        if let Err(e) = ssh_pool_clone.kill_process(host_idx, pid).await {
                            warn!(
                                "Failed to terminate process {} for {}: {}",
                                pid, address, e
                            );
                        }
                    });
                }
                info!(
                    "Removing session for {} (complete: {}, timed out: {})",
                    session.address,
                    session.process_complete,
                    session.start_time.elapsed() >= ITYFUZZ_TIMEOUT
                );
                false
            } else {
                if let Some((host_idx, pid)) = session.process_id {
                    let check_cmd = format!("ps -p {} > /dev/null && echo running || echo stopped", pid);
                    let host = {
                        let clients = ssh_pool.clients.lock().unwrap();
                        clients[host_idx % clients.len()].0.clone()
                    };
                    let address = session.address.clone();
                    let ssh_pool_clone = Arc::clone(ssh_pool);
                    task::spawn(async move {
                        if let Ok((output, _)) = ssh_pool_clone.execute(host_idx, &check_cmd).await {
                            if output.trim() == "stopped" {
                                info!(
                                    "Process {} for {} stopped prematurely on {}",
                                    pid, address, host
                                );
                            }
                        }
                    });
                }
                true
            }
        });
        if before > sessions.len() {
            info!(
                "Cleaned up {} sessions, {} remaining",
                before - sessions.len(),
                sessions.len()
            );
        }
    }
}

async fn check_output_for_vulnerabilities(
    output: &str,
    info: &mut SessionInfo,
    bot: &Arc<Bot>,
    addresses: &str,
) -> Result<bool> {
    let vulnerability_keywords = ["VULNERABILITY", "Found", "Critical", "vulnerabilities", "Arbitrary Call"];
    let _log_path = get_log_path(&info.address, &info.project_name);
    if vulnerability_keywords.iter().any(|&keyword| output.contains(keyword)) {
        info.found_vulnerability = true;

        // Extract vulnerability details
        let vuln_type = output
            .lines()
            .find(|line| line.contains("VULNERABILITY") || line.contains("Arbitrary Call"))
            .map(|line| {
                line.split("**").nth(1).unwrap_or("Unknown").trim_matches(['[', ']']).to_string()
            })
            .unwrap_or("Unknown".to_string());
        let contract_addr = output
            .lines()
            .find(|line| line.contains("0x"))
            .and_then(|line| {
                line.split_whitespace()
                    .find(|s| s.starts_with("0x") && s.len() == 42)
                    .map(|s| s.to_string())
            })
            .unwrap_or(addresses.split(',').next().unwrap_or("Unknown").to_string());

        info.vulnerability_details = Some(VulnerabilityDetails {
            vuln_type: vuln_type.clone(),
            contract_addr: contract_addr.clone(),
        });

        let vulnerability_log_path = get_vulnerability_log_path(&info.project_name);
        let vulnerability_entry = format!(
            "==== VULNERABILITY REPORT ====\n\
             Timestamp: {}\n\
             Project: {}\n\
             Contract Addresses: {}\n\
             Vulnerability Type: {}\n\
             Affected Contract: {}\n\
             \n\
             DETAILS:\n\
             {}\n\
             {}\n\n",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            info.project_name,
            info.address,
            vuln_type,
            contract_addr,
            output,
            "=".repeat(50)
        );

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&vulnerability_log_path)?;
        file.write_all(vulnerability_entry.as_bytes())?;

        let telegram_message = format!(
            "🚨 *VULNERABILITY FOUND*\n\
             *Project*: {}\n\
             *Contracts*: {}\n\
             *Type*: {}\n\
             *Affected*: {}\n\
             *Time*: `{}`",
            info.project_name,
            addresses,
            vuln_type,
            contract_addr,
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let _ = send_telegram_alert(bot, &telegram_message, AlertType::Vulnerability).await;
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn send_telegram_alert(
    bot: &Arc<Bot>,
    message: &str,
    alert_type: AlertType,
) -> Result<teloxide::types::Message> {
    let formatted_message = match alert_type {
        AlertType::Vulnerability => format!(
            "🚨 *VULNERABILITY FOUND*\n{}\nTime: `{}`",
            escape_markdown(message),
            Local::now().format("%Y-%m-%d %H:%M:%S")
        ),
        AlertType::Error => format!("⚠️ *Error*\n{}", escape_markdown(message)),
        AlertType::Info => format!("ℹ️ *Info*\n{}", escape_markdown(message)),
    };

    let result = retry(ExponentialBackoff::default(), || async {
        let send_result = bot
            .send_message(ChatId(TELEGRAM_CHAT_ID), &formatted_message)
            .parse_mode(ParseMode::MarkdownV2)
            .send()
            .await;
        match &send_result {
            Ok(_) => info!("Sent Telegram alert: {}", formatted_message),
            Err(e) => warn!("Failed to send Telegram alert: {}", e),
        }
        Ok(send_result?)
    })
    .await?;

    Ok(result)
}

async fn check_telegram_connection(bot: &Arc<Bot>) -> Result<()> {
    retry(ExponentialBackoff::default(), || async {
        let result = bot
            .send_message(ChatId(TELEGRAM_CHAT_ID), "🔄 *Bot Connection Test*")
            .parse_mode(ParseMode::MarkdownV2)
            .send()
            .await;
        match &result {
            Ok(_) => info!("Telegram connection test successful"),
            Err(e) => warn!("Telegram connection test failed: {}", e),
        }
        Ok(result?)
    })
    .await?;
    Ok(())
}

fn escape_markdown(text: &str) -> String {
    let special_chars = [
        '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
    ];
    let mut escaped = String::with_capacity(text.len() * 2);
    for c in text.chars() {
        if special_chars.contains(&c) {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

#[derive(Debug, Clone)]
enum AlertType {
    Vulnerability,
    Error,
    Info,
}

#[derive(Debug, Clone)]
struct SessionInfo {
    start_time: Instant,
    address: String,
    process_id: Option<(usize, u32)>,
    verified: bool,
    last_check: Instant,
    project_name: String,
    found_vulnerability: bool,
    process_complete: bool,
    last_log_update: Instant,
    vulnerability_details: Option<VulnerabilityDetails>,
}

#[derive(Debug, Clone)]
struct VulnerabilityDetails {
    vuln_type: String,
    contract_addr: String,
}

impl Drop for SessionInfo {
    fn drop(&mut self) {
        if self.process_complete && !self.found_vulnerability {
            let log_path = get_log_path(&self.address, &self.project_name);
            if let Err(e) = fs::remove_file(&log_path) {
                warn!("Failed to remove log file {}: {}", log_path, e);
            }
        }
    }
}
