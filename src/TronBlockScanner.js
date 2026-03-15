import TronWeb from 'tronweb';
import axios from 'axios';
import { TronTransactionParser } from './TronTransactionParser.js';

export class TronBlockScanner {
  constructor(config) {
    this.tronWeb = new TronWeb({ 
      fullHost: config.fullHost, 
      headers: config.headers 
    });
    this.apiKey = config.apiKey;
    this.fullHost = config.fullHost;
    this.confirmThreshold = config.confirmThreshold || 20;
    this.cleanupInterval = config.cleanupInterval || 10 * 60 * 1000;
    this.lastNumber = 0;
    this.scanning = false;
    this.pendingConfirms = new Map();
    this.clients = new Set();
    this.cleanupTimer = null;
  }

  async init() {
    try {
      const block = await this.tronWeb.trx.getCurrentBlock();
      this.lastNumber = block.block_header.raw_data.number;
      console.log(`✅ [TRON] Scanner initialized. Current block: ${this.lastNumber}`);
    } catch (e) {
      console.error(`❌ [TRON] Initialization failed:`, e.message);
      throw e;
    }
  }

  broadcast(obj) {
    const data = JSON.stringify(obj);
    for (const ws of this.clients) {
      if (ws.readyState === 1) ws.send(data);
    }
  }

  async scanBlock(number) {
    try {
      const res = await axios.get(`${this.fullHost}/wallet/getblockbynum`, {
        headers: { 'api-key': this.apiKey },
        params: { num: number },
        timeout: 10000
      });
      
      const block = res.data;
      const txs = block.transactions || [];
      
      this.broadcast({ 
        type: 'block', 
        chain: 'TRX', 
        number, 
        txCount: txs.length,
        timestamp: block.block_header?.raw_data?.timestamp
      });

      for (const tx of txs) {
        const transfer = TronTransactionParser.getTransferInfo(tx);
        const summary = {
          type: 'tx',
          chain: 'TRX',
          block: number,
          txID: tx.txID,
          timestamp: tx.raw_data.timestamp,
          asset: transfer?.asset || null,
          from: transfer?.from || null,
          to: transfer?.to || null,
          amount: transfer?.amount || null,
          amountSun: transfer?.amountSun || null,
          rawAmount: transfer?.rawAmount || null,
          contract: transfer?.contract || null,
          confirmations: 1
        };
        this.broadcast(summary);

        if (!this.pendingConfirms.has(tx.txID) && this.confirmThreshold > 1) {
          this.pendingConfirms.set(tx.txID, { block: number });
        }
      }
    } catch (e) {
      console.error(`⚠️ [TRON] Error scanning block ${number}:`, e.message);
    }
  }

  processConfirmations(currentHeight) {
    if (this.confirmThreshold <= 1) return;

    const toDelete = [];
    for (const [txID, info] of this.pendingConfirms.entries()) {
      const confirmations = currentHeight - info.block + 1;
      if (confirmations >= this.confirmThreshold) {
        this.broadcast({ 
          type: 'confirm', 
          chain: 'TRX', 
          txID, 
          block: info.block, 
          confirmations 
        });
        toDelete.push(txID);
      }
    }
    for (const txID of toDelete) {
      this.pendingConfirms.delete(txID);
    }
  }

  async start(intervalMs = 4000) {
    if (this.scanning) return;
    this.scanning = true;
    console.log(`🚀 [TRON] Scanner started - polling every ${intervalMs}ms`);

    this.cleanupTimer = setInterval(() => {
      const oldSize = this.pendingConfirms.size;
      const cutoffBlock = this.lastNumber - 50;

      for (const [txID, info] of this.pendingConfirms.entries()) {
        if (info.block < cutoffBlock) {
          this.pendingConfirms.delete(txID);
        }
      }

      const cleaned = oldSize - this.pendingConfirms.size;
      if (cleaned > 0) {
        console.log(`🧹 [TRON] Cleaned ${cleaned} old confirmations. Pending: ${this.pendingConfirms.size}`);
      }
    }, this.cleanupInterval);

    while (this.scanning) {
      try {
        const block = await this.tronWeb.trx.getCurrentBlock();
        const current = block.block_header.raw_data.number;
        
        if (current > this.lastNumber) {
          for (let n = this.lastNumber + 1; n <= current; n++) {
            await this.scanBlock(n);
            this.processConfirmations(n);
          }
          this.lastNumber = current;
        }
      } catch (e) {
        console.error(`⚠️ [TRON] Scanner error:`, e.message);
      }
      
      await new Promise(r => setTimeout(r, intervalMs));
    }
  }

  stop() {
    this.scanning = false;
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    console.log(`⏹️ [TRON] Scanner stopped`);
  }
}