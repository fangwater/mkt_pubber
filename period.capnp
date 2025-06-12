@0x9a9b7c8d9e0f1a2b;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("mkt::msg");

struct PriceLevel {
  price @0 :Float64;
  amount @1 :Float64;
}

struct IncrementOrderBookInfo {
  timestamp @0 :Int64;
  isSnapshot @1 :Bool;
  bids @2 :List(PriceLevel);
  asks @3 :List(PriceLevel);
}

struct TradeInfo {
  timestamp @0 :Int64;
  side @1 :Text;
  price @2 :Float64;
  amount @3 :Float64;
}

struct SymbolInfo {
  symbol @0 :Text;
  trades @1 :List(TradeInfo);
  incs @2 :List(IncrementOrderBookInfo);
}

struct PeriodMessage {
  period @0 :Int64;
  ts @1 :Int64;
  postTs @2 :Int64;
  posterId @3 :Text;
  symbolInfos @4 :List(SymbolInfo);
}