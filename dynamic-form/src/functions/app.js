export function extractData(json) {
  const data = [
    json.currentIndex,
    json.metadata.accountNumber,
    json.loanDetails.customer.borrower.customerDetails.individual.fullName,
    json.loanDetails.collectionInfo.collectableAmount,
    json.loanDetails.collectionInfo.principalOutstanding,
    json.loanDetails.collectionInfo.totalOutstanding,
    json.loanDetails.collectionInfo.riskCategory,
  ];
  return data;
}

// const resultArray = extractData(json);
// const resultCSV = resultArray.join(',');
