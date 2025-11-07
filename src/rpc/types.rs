use ethers::prelude::*;
use bigdecimal::BigDecimal;
use crate::models::block::NewBlock;
use crate::models::transaction::NewTransaction;
use std::str::FromStr;

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
    }
}