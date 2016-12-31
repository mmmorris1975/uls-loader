# uls-loader
A process to download FCC ULS data and load to a local database. We use Postgres for the database for it's rich set of data types and the built-in set of geometric processing functions.  On a RPi 2 with a 3Mbit/s internet pipe the whole process takes about 6 hours, more capable hardware and a fatter pipe could cut down that time a bit, but even now it's still something that can be run overnight on some schedule.

## FCC Notes
Per the Intro to ULS Public Access Files (pa_intro24.pdf) new files are available early Sunday morning each week.
