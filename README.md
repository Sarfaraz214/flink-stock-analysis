# flink-stock-analysis

- A Flink application project using Scala and SBT for Stock Analysis.

- In order to run your application from within IntelliJ, open the Configuration drop-down and click on Edit Configuration and set the output file path in Program Arguments.

- Run the DataServer (available under util) before running the Flink program.

- Output file path = location where the output of the analysis will be dumped into as a text file.

- Required input file (Dataset) in present in the "resources" directory.

- Structure of input data: ## Date, Timestamp, CompanyName, StockPrice, StockVolume

-Use case -

       i. For every 1 minute, we need to calculate: a. Maximum trade price b. Minimum trade price c. Maximum trade volume d. Minimum trade volume e. %age change in Max Trade Price from previous 1 minute f. %age change in Max Trade Volume from previous 1 minute

       ii. For every 2 minutes window, raise an alert if Max Trade Price is changed (up or down) by more then 5% (threshold value) and capture that record.
