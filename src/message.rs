use anyhow::{Result, Context, bail};
use log::{info, error, debug, warn};
//for capnp
use capnp::message::ReaderOptions;
use capnp::serialize;
//for protobuf
use crate::proto::message_old;
use prost::Message;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::{Read, Write};

// 引用生成的 Cap'n Proto 代码
use crate::period_capnp;

pub struct PriceLevel {
    pub price: f64,
    pub amount: f64,
}

pub struct IncrementOrderBookInfo {
    pub timestamp: i64,
    pub is_snapshot: bool,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

pub struct TradeInfo {
    pub timestamp: i64,
    pub side: String,
    pub price: f64,
    pub amount: f64,
}

pub struct SymbolInfo {
    pub symbol: String,
    pub trades: Vec<TradeInfo>,
    pub incs: Vec<IncrementOrderBookInfo>,
}

pub struct PeriodMessage {
    pub period: i64,
    pub ts: i64,
    pub post_ts: i64,
    pub poster_id: String,
    pub symbol_infos: Vec<SymbolInfo>,
}

impl PeriodMessage {
    pub fn from_capnp(data: &[u8], is_compressed: bool) -> Result<Self> {
        let data = if is_compressed {
            let mut decoder = ZlibDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            decompressed
        } else {
            data.to_vec()
        };

        let message_reader = serialize::read_message(
            &mut std::io::Cursor::new(data),
            ReaderOptions::new(),
        )?;

        let reader = message_reader.get_root::<period_capnp::period_message::Reader>()?;
        
        Ok(PeriodMessage {
            period: reader.get_period(),
            ts: reader.get_ts(),
            post_ts: reader.get_post_ts(),
            poster_id: reader.get_poster_id()?.to_string()?,
            symbol_infos: reader.get_symbol_infos()?
                .iter()
                .map(|info| -> Result<SymbolInfo> {
                    Ok(SymbolInfo {
                        symbol: info.get_symbol()?.to_string()?,
                        trades: info.get_trades()?
                            .iter()
                            .map(|trade| -> Result<TradeInfo> {
                                Ok(TradeInfo {
                                    timestamp: trade.get_timestamp(),
                                    side: trade.get_side()?.to_string()?,
                                    price: trade.get_price(),
                                    amount: trade.get_amount(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                        incs: info.get_incs()?
                            .iter()
                            .map(|inc| -> Result<IncrementOrderBookInfo> {
                                Ok(IncrementOrderBookInfo {
                                    timestamp: inc.get_timestamp(),
                                    is_snapshot: inc.get_is_snapshot(),
                                    bids: inc.get_bids()?
                                        .iter()
                                        .map(|bid| -> Result<PriceLevel> {
                                            Ok(PriceLevel {
                                                price: bid.get_price(),
                                                amount: bid.get_amount(),
                                            })
                                        })
                                        .collect::<Result<Vec<_>>>()?,
                                    asks: inc.get_asks()?
                                        .iter()
                                        .map(|ask| -> Result<PriceLevel> {
                                            Ok(PriceLevel {
                                                price: ask.get_price(),
                                                amount: ask.get_amount(),
                                            })
                                        })
                                        .collect::<Result<Vec<_>>>()?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn to_capnp(&self, use_compression: bool) -> Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        let mut builder = message.init_root::<period_capnp::period_message::Builder>();
        
        builder.set_period(self.period);
        builder.set_ts(self.ts);
        builder.set_post_ts(self.post_ts);
        builder.set_poster_id(&self.poster_id);
        
        let mut symbol_infos = builder.init_symbol_infos(self.symbol_infos.len() as u32);
        for (i, info) in self.symbol_infos.iter().enumerate() {
            {
                let mut symbol_info = symbol_infos.reborrow().get(i as u32);
                symbol_info.set_symbol(&info.symbol);
                
                let mut trades = symbol_info.reborrow().init_trades(info.trades.len() as u32);
                for (j, trade) in info.trades.iter().enumerate() {
                    let mut trade_builder = trades.reborrow().get(j as u32);
                    trade_builder.set_timestamp(trade.timestamp);
                    trade_builder.set_side(&trade.side);
                    trade_builder.set_price(trade.price);
                    trade_builder.set_amount(trade.amount);
                }
            }
            
            {
                let mut symbol_info = symbol_infos.reborrow().get(i as u32);
                let mut incs = symbol_info.init_incs(info.incs.len() as u32);
                for (j, inc) in info.incs.iter().enumerate() {
                    {
                        let mut inc_builder = incs.reborrow().get(j as u32);
                        inc_builder.set_timestamp(inc.timestamp);
                        inc_builder.set_is_snapshot(inc.is_snapshot);
                        
                        let mut bids = inc_builder.reborrow().init_bids(inc.bids.len() as u32);
                        for (k, bid) in inc.bids.iter().enumerate() {
                            let mut bid_builder = bids.reborrow().get(k as u32);
                            bid_builder.set_price(bid.price);
                            bid_builder.set_amount(bid.amount);
                        }
                    }
                    
                    {
                        let mut inc_builder = incs.reborrow().get(j as u32);
                        let mut asks = inc_builder.init_asks(inc.asks.len() as u32);
                        for (k, ask) in inc.asks.iter().enumerate() {
                            let mut ask_builder = asks.reborrow().get(k as u32);
                            ask_builder.set_price(ask.price);
                            ask_builder.set_amount(ask.amount);
                        }
                    }
                }
            }
        }
        
        let mut buffer = Vec::new();
        serialize::write_message(&mut buffer, &message)?;
        
        if use_compression {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&buffer)?;
            let compressed = encoder.finish()?;
            Ok(compressed)
        } else {
            Ok(buffer)
        }
    }

    pub fn from_protobuf(data: &[u8], is_compressed: bool) -> Result<Self> {
        let data = if is_compressed {
            let mut decoder = ZlibDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            decompressed
        } else {
            data.to_vec()
        };

        let proto_msg = message_old::PeriodMessage::decode(data.as_slice())?;
        
        Ok(PeriodMessage {
            period: proto_msg.period,
            ts: proto_msg.ts,
            post_ts: proto_msg.post_ts,
            poster_id: proto_msg.poster_id,
            symbol_infos: proto_msg.symbol_infos.into_iter().map(|info| SymbolInfo {
                symbol: info.symbol,
                trades: info.trades.into_iter().map(|trade| TradeInfo {
                    timestamp: trade.timestamp,
                    side: trade.side,
                    price: trade.price,
                    amount: trade.amount,
                }).collect(),
                incs: info.incs.into_iter().map(|inc| IncrementOrderBookInfo {
                    timestamp: inc.timestamp,
                    is_snapshot: inc.is_snapshot,
                    bids: inc.bids.into_iter().map(|bid| PriceLevel {
                        price: bid.price,
                        amount: bid.amount,
                    }).collect(),
                    asks: inc.asks.into_iter().map(|ask| PriceLevel {
                        price: ask.price,
                        amount: ask.amount,
                    }).collect(),
                }).collect(),
            }).collect(),
        })
    }

    pub fn to_protobuf(&self, use_compression: bool) -> Result<Vec<u8>> {
        let proto_msg = message_old::PeriodMessage {
            period: self.period,
            ts: self.ts,
            post_ts: self.post_ts,
            poster_id: self.poster_id.clone(),
            symbol_infos: self.symbol_infos.iter().map(|info| message_old::SymbolInfo {
                symbol: info.symbol.clone(),
                trades: info.trades.iter().map(|trade| message_old::TradeInfo {
                    timestamp: trade.timestamp,
                    side: trade.side.clone(),
                    price: trade.price,
                    amount: trade.amount,
                }).collect(),
                incs: info.incs.iter().map(|inc| message_old::IncrementOrderBookInfo {
                    timestamp: inc.timestamp,
                    is_snapshot: inc.is_snapshot,
                    bids: inc.bids.iter().map(|bid| message_old::PriceLevel {
                        price: bid.price,
                        amount: bid.amount,
                    }).collect(),
                    asks: inc.asks.iter().map(|ask| message_old::PriceLevel {
                        price: ask.price,
                        amount: ask.amount,
                    }).collect(),
                }).collect(),
            }).collect(),
        };

        let mut buffer = Vec::new();
        proto_msg.encode(&mut buffer)?;
        
        if use_compression {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&buffer)?;
            let compressed = encoder.finish()?;
            Ok(compressed)
        } else {
            Ok(buffer)
        }
    }

    pub fn print_info(&self) {
        info!("Period Message Info:");
        info!("  Period: {}", self.period);
        info!("  Timestamp: {}", self.ts);
        info!("  Post Timestamp: {}", self.post_ts);
        info!("  Poster ID: {}", self.poster_id);
        info!("  Number of Symbols: {}", self.symbol_infos.len());
        println!("┌{:─^60}┐", "");
        println!("│{: ^60}│", "OrderBook Archive");
        println!("│{: ^60}│", format!("period: {}", self.period));
        println!("├{:─^60}┤", "");
        println!("│{: <20}│{: >19}│{: >19}│", "Symbol", "Inc_Count", "Trade_Count");
        println!("├{:─^60}┤", "");
        for symbol_info in &self.symbol_infos {
            println!("│{: <20}│{: >19}│{: >19}│", 
                symbol_info.symbol, 
                symbol_info.incs.len(), 
                symbol_info.trades.len()
            );
        }
        println!("└{:─^60}┘", "");
    }
} 