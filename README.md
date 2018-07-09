# IntegratedStationDatasetScrapper
Downloads, gathers, and condenses data from the Integrated Station Dataset.

I was learning about machine learning, and I was looking for a project that I could implement machine learning into. I stumbled across data.gov and saw that there was new data and nearly real time tracking of the US energy production and consumption rates and came up with the idea of making a modle that would predict energy consumption. I was working with the hypothesis that the greatest variance in energy consumption between days would be heating or AC usages. That is determined mainly by the temperature that day. I decided to use the weather as my first input, see how much if any correlation is there, and go from there.

I found the Integrated Station Dataset, which are logs of weather stations that log different weather data. I created these packages to download those needed data files, read and save the desired data, and also distill the data so that instead of hourly data, it will give you daily or monthly data condensed by using whatever statistical function you give it.

After getting this package finished, I went to start creating the package to get the energy data which I would use to train the machine learning algorithm. I then realized that the energy data is tracked in almost real time, but it is only archived monthly. Or at least monthly was all that I could find archived for download. Monthly data wouldn’t be as useful as daily, and it also wouldn’t give me enough data to train the algorithm. So I decided that I would cancel the larger project, but put the scraper for the Integrated Station Dataset up in my portfolio.
