# Byzantine Fault-Tolerant Replicated State Machine Library

> **Fork notice.** This is a fork of
> [hyperledger-labs/SmartBFT](https://github.com/hyperledger-labs/SmartBFT), re-pathed as
> `github.com/SmartBFT-Go/smartbft-core` so downstream modules can depend on it without a
> `replace` directive. It carries changes required by a Byzantine-fault-tolerant key/value
> store: proposal digests are produced by
> [canonical](https://github.com/SmartBFT-Go/canonical) rather than by calling
> `encoding/asn1` directly, and CI adds lint and determinism gates. The wire encoding is
> unchanged. Upstream remains the reference for the protocol itself.




This is a Byzantine fault-tolerant (BFT) state machine replication (SMR) library. 
It is an open source library written in Go.
The implementation is inspired by the [BFT-SMaRt project](https://github.com/bft-smart/library). 
For more information on this library see our [wiki page](https://github.com/hyperledger-labs/SmartBFT/wiki).


## License

The source code files are made available under the Apache License, Version 2.0 (Apache-2.0), located in the [LICENSE](LICENSE) file.


## Contact

* Yacov Manevich - [yacovm@il.ibm.com](mailto:yacovm@il.ibm.com)
* Hagar Meir - [hagar.meir@ibm.com](mailto:hagar.meir@ibm.com)
* Artem Barger - [bartem@il.ibm.com](mailto:bartem@il.ibm.com)
* Yoav Tock - [tock@il.ibm.com](mailto:tock@il.ibm.com)
