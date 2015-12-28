USER_AGENT = 'Python-urllib/3.4'
USER_AGENT_INFO = {
          'name' : 'python crawler',
          'organisation': '-',
          'location' : 'Unknown',
          'language' : 'Python 3'
}

SITES = [
    # 'http://daskapital.nl',
    # 'http://debuitenlandredactie.nl',
    # 'http://langleveeuropa.nl',
    # 'http://nurksmagazine.nl',
    # 'http://obsession-magazine.nl',
    # 'http://sportkranten.startpagina.nl',
    # 'http://tijdschrift.startpagina.nl',
    # 'http://werkgroepcaraibischeletteren.nl',
    # 'http://www.112noordholland.nl',
    # 'http://www.7ditches.tv',
    # 'http://www.apache.be',
    # 'http://www.bloemenkrant.nl',
    # 'http://www.denoordoostpolder.nl',
    # 'http://www.earth-matters.nl',
    # 'http://www.geenstijl.nl',
    # 'http://www.grenswetenschap.nl',
    # 'http://www.ikonrtv.nl',
    # 'http://www.mensenlinq.nl',
    # 'http://www.noordhollandsdagblad.nl',
    # 'http://www.nrclux.nl',
    # 'http://www.nrcq.nl',
    # 'http://www.omroepbrabant.nl',
    # 'http://www.omroepflevoland.nl',
    # 'http://www.omroepgelderland.nl',
    # 'http://www.omroepwest.nl',
    # 'http://www.omroepzeeland.nl',
    # 'http://www.ouderenjournaal.nl',
    # 'http://www.papermagazine.nl',
    # 'http://www.powned.tv',
    # 'http://www.radio10.sr',
    # 'http://www.ravage-webzine.nl',
    # 'http://www.republiekallochtonie.nl',
    # 'http://www.rtvdrenthe.nl',
    # 'http://www.rtvnh.nl',
    # 'http://www.rtvoost.nl',
    # 'http://www.rtvutrecht.nl'
    # 'http://www.sanomamedia.nl',
    # 'http://www.spiegel.de',
    # 'http://www.standaard.be',
    # 'http://www.stoptax.nl',
    # 'http://www.telesport.nl',
    # 'http://www.theaterkrant.nl',
    # 'http://www.thesun.co.uk',
    # 'http://www.tweedekamer.nl',
    # 'https://decorrespondent.nl',
    ### sites below is a full list of sites:
    'http://112regiodrenthe.nl',
    'http://allesoverutrecht.nl',
    'http://baarn.startpagina.nl',
    'http://biflatie.nl',
    'http://brekend.nl',
    'http://climategate.nl',
    'http://curiales.nl',
    'http://dedrontenaar.nl',
    'http://fd.nl',
    'http://leiden.courant.nu',
    'http://mwnw.nl',
    'http://nederlandsmedianieuws.nl',
    'http://nos.nl',
    'http://nos.startpagina.nl',
    'http://rtvzeewolde.nl',
    'http://sargasso.nl',
    'http://secureomg.nl',
    'http://stanvanhoucke.blogspot.nl',
    'http://tpo.nl',
    'http://vojn.nl',
    'http://www.112brabant.nl',
    'http://www.112fryslan.nl',
    'http://www.112groningen.nl',
    'http://www.112noordholland.nl',
    'http://www.ad.nl',
    'http://www.agriholland.nl',
    'http://www.archiefleeuwardercourant.nl',
    'http://www.at5.nl',
    'http://www.barneveldsekrant.nl',
    'http://www.bd.nl',
    'http://www.bhznet.nl',
    'http://www.binnenvaartkrant.nl',
    'http://www.bladen.nl',
    'http://www.bndestem.nl',
    'http://www.bnr.nl',
    'http://www.boerderij.nl',
    'http://www.buienradar.nl',
    'http://www.bureau-redactie.nl',
    'http://www.dagelijksestandaard.nl',
    'http://www.dasmooi.nl',
    'http://www.de-midweek.nl',
    'http://www.debeurs.nl',
    # 'http://www.delpher.nl',   # only available for research
    'http://www.denoordoostpolder.nl',
    'http://www.destadamersfoort.nl',
    'http://www.destentor.nl',
    'http://www.dutchnews.nl',
    'http://www.dvhn.nl',
    'http://www.ed.nl',
    'http://www.edestad.nl',
    'http://www.eemsbode.nl',
    'http://www.elsevier.nl',
    'http://www.emmen.nu',
    'http://www.flakkeenieuws.nl',
    'http://www.flevopost.nl',
    'http://www.frieschdagblad.nl',
    'http://www.ftm.nl',
    'http://www.gelderlander.nl',
    'http://www.gelderlandinbeeld.nl',
    'http://www.gewoon-nieuws.nl',
    'http://www.gezinsbode.nl',
    'http://www.gooieneemlander.nl',
    'http://www.groene.nl',
    'http://www.haarlemsdagblad.nl',
    'http://www.harenerweekblad.nl',
    'http://www.headlines24.nl',
    'http://www.het-westerkwartier.nl',
    'http://www.hetgezinsblad.nl',
    'http://www.hetstreekblad.nl',
    'http://www.heturkerland.nl',
    'http://www.hln.be',
    'http://www.hpdetijd.nl',
    'http://www.hskrant.nl',
    'http://www.hvzeeland.nl',
    'http://www.ijmuidercourant.nl',
    'http://www.joop.nl',
    'http://www.kanaalstreek.nl',
    'http://www.katholieknieuwsblad.nl',
    'http://www.knmi.nl',
    'http://www.krantvancoevorden.nl',
    'http://www.krantvanflevoland.nl',
    'http://www.krantvanhoogeveen.nl',
    'http://www.l1.nl',
    'http://www.lc.nl',
    'http://www.leidschdagblad.nl',
    'http://www.limburger.nl',
    'http://www.limburgsecourant.nl',
    'http://www.loyalist.nl',
    'http://www.marknesse.nl',
    'http://www.meppelercourant.nl',
    'http://www.metronieuws.nl',
    'http://www.nd.nl',
    'http://www.nieuwsblad.be',
    'http://www.nieuwsbladtransport.nl',
    'http://www.nieuwsgrazer.nl',
    'http://www.nijmegennieuws.nl',
    'http://www.ninefornews.nl',
    'http://www.nltimes.nl',
    'http://www.noorderkrant.nl',
    'http://www.noordhollandsdagblad.nl',
    'http://www.nrc.nl',
    'http://www.nu.nl',
    'http://www.nujij.nl',
    'http://www.omroepbrabant.nl',
    'http://www.omroepflevoland.nl',
    'http://www.omroepgelderland.nl',
    'http://www.omroepwest.nl',
    'http://www.omroepzeeland.nl',
    'http://www.omropfryslan.nl',
    'http://www.parool.nl',
    'http://www.pen.nl',
    'http://www.pzc.nl',
    'http://www.quotenet.nl',
    'http://www.radiolelystad.nl',
    'http://www.refdag.nl',
    'http://www.roderjournaal.nl',
    'http://www.rtlnieuws.nl',
    'http://www.rtvdrenthe.nl',
    'http://www.rtvnh.nl',
    'http://www.rtvoost.nl',
    'http://www.rtvutrecht.nl',
    'http://www.scherpenzeelsekrant.nl',
    'http://www.schuttevaer.nl',
    'http://www.sikkom.nl',
    'http://www.spitsnieuws.nl',
    'http://www.stellingwerf.nl',
    'http://www.telegraaf.nl',
    'http://www.terapelercourant.nl',
    'http://www.thehollandtimes.nl',
    'http://www.trouw.nl',
    'http://www.tubantia.nl',
    'http://www.vaarkrant.nl',
    'http://www.veendammer.nl',
    'http://www.veenendaalsekrant.nl',
    'http://www.vn.nl',
    'http://www.volkskrant.nl',
    'http://www.vrijspreker.nl',
    'http://www.waldnet.nl',
    'http://www.wanttoknow.nl',
    'http://www.waterkant.net',
    'http://www.wel.nl',
    'http://www.z24.nl',
    'http://www.zuidoosthoeker.nl/',
    'http://xandernieuws.punt.nl',
    'https://www.villamedia.nl',
]

ALWAYS_INCLUDE_BASE_IN_CRAWLABLE_LINK_QUEUE = False
CRAWL_DEPTH = 0         # depth of links followed outside of base url
CRAWL_DELAY = 5         # seconds of waiting time for each time crawled
REVISIT_AFTER = 15      # revisit time in days
MAX_THREADS = 50       # number of threads running at once

DATE_TIME_DISTANCE = 4  # allowed distance in characters between date and time

VERBOSE = False
DATABASE_FILENAME = 'nieuwscrawltest.sqlite3'
LOG_FILENAME = 'nieuwscrawltest.log'
RESET_DATABASE = False

NOFOLLOW = [
    "creativecommons",
    "facebook",
    "feedly",
    "flickr",
    "github",
    "google",
    "instagram",
    "last.fm",
    "linkedin",
    "mozzila",
    "openstreetmap",
    "opera",
    "sciencedirect",
    "twitter",
    "vimeo",
    "wikimedia",
    "wikipedia",
    "wiley",
    "youtube",
    'sciencecommons'
]
