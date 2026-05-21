## Executive Overview

### Map visual: Azure Maps unavailable in region

Azure Maps (the recommended replacement for the deprecated classic 
Map visual) seems to not enabled in Uruguay due to Microsoft regional 
service availability. The classic `Map` (Bing-based) visual was 
retained. Functionally equivalent: country-level geocoding, bubble
size by customer count, fully interactive with slicers. The 
deprecation notice in PowerBI is informational; no end-of-life
date is announced. Documented as a known limitation for any future 
reviewer who tries to refresh the .pbix in a different region.