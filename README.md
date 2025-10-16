"We do not retroactively reassign symbols in our historical data. Symbols found in our historical data are exactly as they appeared in the live data at the original event time."
So if FB changed to META, the historical data still says "FB" for the old dates. That's point-in-time accurate, which is good for backtesting actually.
But the problem the user is raising is: how do you follow a company through time if its symbol changes? 
Let me think about what Databento actually provides. They must have some way to track instruments across symbol changes. Let me reconsider...
Actually, I think the key might be that Databento doesn't provide a persistent cross-symbol-change identifier in their basic data. You might need to:
Use their definition/reference schema to track symbol changes
Build your own mapping
Or use raw_symbol with point-in-time awareness