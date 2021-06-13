username='LW5685'
password='Sreeja@28'
pin='453945'

try:
    enctoken = open('enctoken.txt', 'r').read().rstrip()
except Exception as e:
    print('Exception occurred :: {}'.format(e))
    enctoken = None
