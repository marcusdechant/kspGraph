#ColorOut.py

def CO():

    # makes exception hook colorful and easy to read. super helpful.

    import sys
    from IPython.core.ultratb import ColorTB

    sys.excepthook=ColorTB() 

CO()

if __name__=="__main__":
    CO()