def getValuesInRange(self, minval, maxval, data: pd.DataFrame):
    return data.between(left=minval, right=maxval).sum()