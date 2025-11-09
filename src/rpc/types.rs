use ethers::prelude::*;
use bigdecimal::BigDecimal;
use crate::models::block::NewBlock;
use crate::models::transaction::NewTransaction;
use std::str::FromStr;

// Bridge contract addresses for direction detection
const TAIKO_L1_BRIDGE_CONTRACTS: &[&str] = &[
    "0xd60247c6848b7ca29eddf63aa924e53db6ddd8ec",  // Main L1 bridge
    "0x1670000000000000000000000000000000000001",  // L2 bridge
    "0x0000777700000000000000000000000000000001",  // Signal service
    "0xEf9EaA1dd30a9AA1df01c36411b5F082aA65fBaa",  // Taiko Token Bridge
    "0xa51894664a773981c6c112c43ce576f315d5b1b6",  // Taiko: WETH Token (withdrawal contract)
];

// Taiko bridge function selectors (validated with TaikoScan)
const DEPOSIT_SELECTORS: &[&str] = &[
    "0x2035065e",  // processMessage - L1→L2 deposits (IN to Taiko) 
    "0x1bdb0037",  // sendMessage - L1→L2 deposits (IN to Taiko)
    "0xd0e30db0",  // deposit() - WETH deposits (IN to Taiko)
];

const WITHDRAW_SELECTORS: &[&str] = &[
    "0x2e1a7d4d",  // withdraw(uint256) - WETH withdrawals (OUT from Taiko) - validated with TaikoScan
];

fn determine_transaction_direction(
    from_address: &str, 
    to_address: Option<&str>, 
    input_data: &[u8],
    receipt: Option<&TransactionReceipt>
) -> String {
    // Check if it involves bridge contracts
    let is_from_bridge = TAIKO_L1_BRIDGE_CONTRACTS.iter().any(|&addr| 
        from_address.to_lowercase() == addr.to_lowercase()
    );
    
    let is_to_bridge = to_address.map_or(false, |to| 
        TAIKO_L1_BRIDGE_CONTRACTS.iter().any(|&addr| 
            to.to_lowercase() == addr.to_lowercase()
        )
    );
    
    // Check for specific bridge function calls by analyzing the function selector
    let (is_deposit_call, is_withdraw_call) = if input_data.len() >= 4 {
        let selector = format!("0x{}", hex::encode(&input_data[0..4]));
        let is_deposit = DEPOSIT_SELECTORS.iter().any(|&sel| sel == selector);
        let is_withdraw = WITHDRAW_SELECTORS.iter().any(|&sel| sel == selector);
        (is_deposit, is_withdraw)
    } else {
        (false, false)
    };
    
    // Analyze logs for bridge events if we have receipt
    let has_bridge_events = if let Some(receipt) = receipt {
        receipt.logs.iter().any(|log| {
            TAIKO_L1_BRIDGE_CONTRACTS.iter().any(|&addr| 
                format!("0x{:x}", log.address).to_lowercase() == addr.to_lowercase()
            )
        })
    } else {
        false
    };
    
    // Enhanced direction detection logic
    // Taiko is L2, so we determine direction from Taiko's perspective:
    
    // HIGH PRIORITY: Explicit withdraw function calls (WETH withdrawals, bridge withdrawals)
    if is_withdraw_call && is_to_bridge {
        return "out".to_string(); // L2→L1 withdrawal = OUT (money leaving Taiko)
    }
    
    // HIGH PRIORITY: Explicit deposit function calls (bridge deposits, WETH deposits)
    if is_deposit_call && is_to_bridge {
        return "in".to_string(); // L1→L2 deposit = IN (money coming into Taiko)
    }
    
    // FALLBACK: Bridge-related but no specific function detected = internal
    if is_to_bridge || is_from_bridge || has_bridge_events {
        return "inside".to_string(); // Bridge admin functions, etc.
    }
    
    // DEFAULT: All other transactions = internal
    "inside".to_string()
}

pub fn block_to_new_block(block: &Block<Transaction>) -> NewBlock {
    let difficulty = BigDecimal::from_str(&block.difficulty.to_string()).unwrap_or_else(|_| BigDecimal::from(0));
    
    let total_difficulty = block.total_difficulty
        .map(|td| BigDecimal::from_str(&td.to_string()).unwrap_or_else(|_| BigDecimal::from(0)));

    NewBlock {
        number: block.number.unwrap().as_u64() as i64,
        hash: format!("0x{:x}", block.hash.unwrap()),
        parent_hash: format!("0x{:x}", block.parent_hash),
        timestamp: block.timestamp.as_u64() as i64,
        gas_limit: block.gas_limit.as_u64() as i64,
        gas_used: block.gas_used.as_u64() as i64,
        miner: format!("0x{:x}", block.author.unwrap_or_default()),
        difficulty,
        total_difficulty,
        size: block.size.map(|s| s.as_u64() as i64),
        transaction_count: block.transactions.len() as i32,
        extra_data: Some(hex::encode(&block.extra_data)),
        logs_bloom: block.logs_bloom.map(|lb| format!("0x{:x}", lb)),
        mix_hash: block.mix_hash.map(|mh| format!("0x{:x}", mh)),
        nonce: block.nonce.map(|n| format!("0x{:x}", n)),
        base_fee_per_gas: block.base_fee_per_gas.map(|bf| bf.as_u64() as i64),
    }
}

pub fn transaction_to_new_transaction(
    tx: &Transaction, 
    block_number: i64, 
    block_hash: &str, 
    transaction_index: i32,
    receipt: Option<&TransactionReceipt>
) -> NewTransaction {
    let gas_price = tx.gas_price.map(|gp| BigDecimal::from_str(&gp.to_string()).unwrap_or_else(|_| BigDecimal::from(0)));
    let max_fee_per_gas = tx.max_fee_per_gas.map(|mf| BigDecimal::from_str(&mf.to_string()).unwrap_or_else(|_| BigDecimal::from(0)));
    let max_priority_fee_per_gas = tx.max_priority_fee_per_gas.map(|mp| BigDecimal::from_str(&mp.to_string()).unwrap_or_else(|_| BigDecimal::from(0)));
    let value = BigDecimal::from_str(&tx.value.to_string()).unwrap_or_else(|_| BigDecimal::from(0));
    
    let (gas_used, status, cumulative_gas_used, effective_gas_price, logs_count, contract_address) = if let Some(receipt) = receipt {
        (
            receipt.gas_used.map(|gu| gu.as_u64() as i64),
            receipt.status.map(|s| s.as_u64() as i32),
            Some(receipt.cumulative_gas_used.as_u64() as i64),
            receipt.effective_gas_price.map(|egp| BigDecimal::from_str(&egp.to_string()).unwrap_or_else(|_| BigDecimal::from(0))),
            Some(receipt.logs.len() as i32),
            receipt.contract_address.map(|ca| format!("0x{:x}", ca))
        )
    } else {
        (None, None, None, None, Some(0), None)
    };

    // Determine transaction direction
    let from_address = format!("0x{:x}", tx.from);
    let to_address_str = tx.to.map(|to| format!("0x{:x}", to));
    let direction = determine_transaction_direction(
        &from_address,
        to_address_str.as_deref(),
        &tx.input,
        receipt
    );

    NewTransaction {
        hash: format!("0x{:x}", tx.hash),
        block_number,
        block_hash: block_hash.to_string(),
        transaction_index,
        from_address: format!("0x{:x}", tx.from),
        to_address: tx.to.map(|to| format!("0x{:x}", to)),
        value,
        gas_limit: tx.gas.as_u64() as i64,
        gas_used,
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        nonce: tx.nonce.as_u64() as i64,
        input_data: if tx.input.is_empty() { None } else { Some(hex::encode(&tx.input)) },
        status,
        contract_address,
        logs_count,
        cumulative_gas_used,
        effective_gas_price,
        transaction_type: tx.transaction_type.map(|tt| tt.as_u64() as i32),
        access_list: tx.access_list.as_ref().map(|al| serde_json::to_string(al).unwrap_or_default()),
        direction,
    }
}