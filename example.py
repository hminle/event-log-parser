from elparser.eventLogParser import EventLogParser

eventLogParser = EventLogParser('./BPI_Challenge_2013_incidents.csv')
eventLogParser.loadEventLog()
df = eventLogParser.buildDataFrame()
print(df[:20])