#!/bin/bash
pathmodule=XXX.h5 # XXX.h5 -> # clothes5_4310.h5
pathdatum=XXX.jpg # XXX.jpg -> # Brain_suit.jpg


#========================= 「XXX.h5」 -> 「$pathmodule」
sed -i "s/XXX.h5/$pathmodule/g" identify.py

#========================= 「XXX.jpg」 -> 「$pathdatum」
sed -i "s/XXX.jpg/$pathdatum/g" identify.py
