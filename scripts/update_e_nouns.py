#!/usr/bin/env python3
"""Fill translation_en and pronunciation for NOUN TSVs starting with E."""

import csv
import os

BASE_DIR = os.path.join('vocabulary', 'dataframes', 'NOUN')

UPDATES = {
    # file -> {text -> {'pronunciation': str, 'translation_en': str}}
    'e-mail.tsv': {
        'e-mail': {
            'pronunciation': 'EE-mail',
            'translation_en': 'email',
        },
    },
    'e.tsv': {
        'e': {
            'pronunciation': 'EH',
            'translation_en': 'letter e',
        },
    },
    'ebbrezza.tsv': {
        'ebbrezza': {
            'pronunciation': 'ehb-BREHTS-tsah',
            'translation_en': 'drunkenness; intoxication',
        },
    },
    'ebola.tsv': {
        'ebola': {
            'pronunciation': 'EH-boh-lah',
            'translation_en': 'Ebola',
        },
    },
    'ebollizione.tsv': {
        'ebollizione': {
            'pronunciation': 'eh-bohl-lee-TSEE-oh-neh',
            'translation_en': 'boiling; boil (scientific)',
        },
    },
    'ebraico.tsv': {
        'ebraico': {
            'pronunciation': 'eh-BRAH-ee-koh',
            'translation_en': 'Hebrew (language)',
        },
    },
    'ebrea.tsv': {
        'ebrea': {
            'pronunciation': 'eh-BREH-ah',
            'translation_en': 'Jew (feminine)',
        },
    },
    'ebreo.tsv': {
        'ebreo': {
            'pronunciation': 'eh-BREH-oh',
            'translation_en': 'Jew (masculine)',
        },
        'ebrei': {
            'pronunciation': 'eh-BRAY-ee',
            'translation_en': 'Jews (masculine)',
        },
    },
    'ebrezza.tsv': {
        'ebrezza': {
            'pronunciation': 'eh-BREHTS-tsah',
            'translation_en': 'intoxication; rapture',
        },
    },
    'ecc..tsv': {
        'ecc.': {
            'pronunciation': 'eht-CHET-teh-rah',
            'translation_en': 'etc.; and so on',
        },
    },
    'eccesso.tsv': {
        'eccesso': {
            'pronunciation': 'eht-CHEHS-soh',
            'translation_en': 'excess',
        },
    },
    'eccezione.tsv': {
        'eccezione': {
            'pronunciation': 'eht-cheh-TSEE-oh-neh',
            'translation_en': 'exception',
        },
        'eccezioni': {
            'pronunciation': 'eht-cheh-TSEE-oh-nee',
            'translation_en': 'exceptions',
        },
    },
    'eccitazione.tsv': {
        'eccitazione': {
            'pronunciation': 'eht-chee-tah-TSEE-oh-neh',
            'translation_en': 'excitement; arousal',
        },
    },
    'ecclesiastico.tsv': {
        'ecclesiastico': {
            'pronunciation': 'ehk-kleh-zee-AHS-tee-koh',
            'translation_en': 'clergyman; ecclesiastic',
        },
    },
    'eclisse.tsv': {
        'eclisse': {
            'pronunciation': 'eh-KLEES-seh',
            'translation_en': 'eclipse',
        },
    },
    'eclissi.tsv': {
        'eclissi': {
            'pronunciation': 'eh-KLEES-see',
            'translation_en': 'eclipse',
        },
    },
    'eco.tsv': {
        'eco': {
            'pronunciation': 'EH-koh',
            'translation_en': 'echo',
        },
    },
    'ecologia.tsv': {
        'ecologia': {
            'pronunciation': 'eh-koh-loh-JEE-ah',
            'translation_en': 'ecology',
        },
    },
    'economia.tsv': {
        'economia': {
            'pronunciation': 'eh-koh-noh-MEE-ah',
            'translation_en': 'economy; economics',
        },
    },
    'economista.tsv': {
        'economista': {
            'pronunciation': 'eh-koh-noh-MEE-stah',
            'translation_en': 'economist',
        },
    },
    'edera.tsv': {
        'edera': {
            'pronunciation': 'EH-deh-rah',
            'translation_en': 'ivy',
        },
    },
    'edicola.tsv': {
        'edicola': {
            'pronunciation': 'eh-DEE-koh-lah',
            'translation_en': 'newsstand; kiosk',
        },
    },
    'edificio.tsv': {
        'edificio': {
            'pronunciation': 'eh-dee-FEE-choh',
            'translation_en': 'building',
        },
        'edifici': {
            'pronunciation': 'eh-dee-FEE-chee',
            'translation_en': 'buildings',
        },
    },
    'editing.tsv': {
        'editing': {
            'pronunciation': 'EH-dee-ting',
            'translation_en': 'editing',
        },
    },
    'editore.tsv': {
        'editore': {
            'pronunciation': 'eh-dee-TOH-reh',
            'translation_en': 'publisher (masculine)',
        },
    },
    'edizione.tsv': {
        'edizione': {
            'pronunciation': 'eh-dee-TSEE-oh-neh',
            'translation_en': 'edition; issue',
        },
    },
    'educazione.tsv': {
        'educazione': {
            'pronunciation': 'eh-doo-kah-TSEE-oh-neh',
            'translation_en': 'education; manners',
        },
    },
    'efemere.tsv': {
        'efemere': {
            'pronunciation': 'eh-FEH-meh-reh',
            'translation_en': 'mayflies (plural)',
        },
    },
    'effemeridio.tsv': {
        'effemeridi': {
            'pronunciation': 'eh-feh-meh-REE-dee',
            'translation_en': 'ephemerides (plural)',
        },
    },
    'effemminato.tsv': {
        'effemminato': {
            'pronunciation': 'eh-fehm-mee-NAH-toh',
            'translation_en': 'effeminate man (masculine)',
        },
        'effemminati': {
            'pronunciation': 'eh-fehm-mee-NAH-tee',
            'translation_en': 'effeminate men (masculine)',
        },
    },
    'effetto.tsv': {
        'effetto': {
            'pronunciation': 'eh-FEHT-toh',
            'translation_en': 'effect',
        },
        'effetti': {
            'pronunciation': 'eh-FEHT-tee',
            'translation_en': 'effects',
        },
    },
    'efficacia.tsv': {
        'efficacia': {
            'pronunciation': 'eh-fee-KAH-chah',
            'translation_en': 'effectiveness; efficacy',
        },
    },
    'efficienza.tsv': {
        'efficienza': {
            'pronunciation': 'eh-fee-CHEN-tsah',
            'translation_en': 'efficiency',
        },
    },
    'eggnog.tsv': {
        'eggnog': {
            'pronunciation': 'EG-nog',
            'translation_en': 'eggnog',
        },
    },
    'egiziano.tsv': {
        'egiziano': {
            'pronunciation': 'eh-jee-TSAH-noh',
            'translation_en': 'Egyptian (masculine)',
        },
    },
    'egizio.tsv': {
        'egizi': {
            'pronunciation': 'eh-JEE-tsee',
            'translation_en': 'Egyptians (masculine)',
        },
    },
    'ego.tsv': {
        'ego': {
            'pronunciation': 'EH-goh',
            'translation_en': 'ego',
        },
    },
    'egoismo.tsv': {
        'egoismo': {
            'pronunciation': 'eh-goh-EEZ-moh',
            'translation_en': 'egoism; selfishness',
        },
    },
    'egoista.tsv': {
        'egoista': {
            'pronunciation': 'eh-goh-EE-stah',
            'translation_en': 'selfish person; egoist',
        },
    },
    'eguaglianza.tsv': {
        'eguaglianza': {
            'pronunciation': 'eh-gwah-LYAHN-tsah',
            'translation_en': 'equality',
        },
    },
    'eguale.tsv': {
        'eguali': {
            'pronunciation': 'eh-GWAH-lee',
            'translation_en': 'equals (plural)',
        },
    },
    'elaborare.tsv': {
        'elaborato': {
            'pronunciation': 'eh-lah-boh-RAH-toh',
            'translation_en': 'paper; essay',
        },
    },
    'elefante.tsv': {
        'elefante': {
            'pronunciation': 'eh-leh-FAHN-teh',
            'translation_en': 'elephant',
        },
        'elefanti': {
            'pronunciation': 'eh-leh-FAHN-tee',
            'translation_en': 'elephants',
        },
    },
    'elementare.tsv': {
        'elementari': {
            'pronunciation': 'eh-leh-men-TAH-ree',
            'translation_en': 'elementary school (plural)',
        },
    },
    'elemento.tsv': {
        'elementi': {
            'pronunciation': 'eh-leh-MEN-tee',
            'translation_en': 'elements',
        },
        'elemento': {
            'pronunciation': 'eh-leh-MEN-toh',
            'translation_en': 'element',
        },
    },
    'elenco.tsv': {
        'elenco': {
            'pronunciation': 'eh-LEN-koh',
            'translation_en': 'list; inventory',
        },
    },
    'eletto.tsv': {
        'eletti': {
            'pronunciation': 'eh-LEHT-tee',
            'translation_en': 'the elect; chosen ones (plural)',
        },
    },
    'elettricista.tsv': {
        'elettricista': {
            'pronunciation': 'eh-leht-tree-CHEES-tah',
            'translation_en': 'electrician',
        },
    },
    'elettricità.tsv': {
        'elettricità': {
            'pronunciation': 'eh-leht-tree-chee-TAH',
            'translation_en': 'electricity',
        },
    },
    'elettrodomestico.tsv': {
        'elettrodomestici': {
            'pronunciation': 'eh-leht-troh-doh-MEH-stee-chee',
            'translation_en': 'home appliances',
        },
        'elettrodomestico': {
            'pronunciation': 'eh-leht-troh-doh-MEH-stee-koh',
            'translation_en': 'home appliance',
        },
    },
    'elettrone.tsv': {
        'elettroni': {
            'pronunciation': 'eh-leht-TROH-nee',
            'translation_en': 'electrons',
        },
    },
    'elezione.tsv': {
        'elezioni': {
            'pronunciation': 'eh-leh-TSYOH-nee',
            'translation_en': 'elections',
        },
        'elezione': {
            'pronunciation': 'eh-leh-TSYOH-neh',
            'translation_en': 'election',
        },
    },
    'elfo.tsv': {
        'elfi': {
            'pronunciation': 'EHL-fee',
            'translation_en': 'elves',
        },
        'elfo': {
            'pronunciation': 'EHL-foh',
            'translation_en': 'elf',
        },
    },
    'elicottero.tsv': {
        'elicottero': {
            'pronunciation': 'eh-lee-KOHT-teh-roh',
            'translation_en': 'helicopter',
        },
        'elicotteri': {
            'pronunciation': 'eh-lee-KOHT-teh-ree',
            'translation_en': 'helicopters',
        },
    },
    'elio.tsv': {
        'elio': {
            'pronunciation': 'EH-lyoh',
            'translation_en': 'helium',
        },
    },
    'elite.tsv': {
        'elite': {
            'pronunciation': 'eh-LEET',
            'translation_en': 'elite',
        },
    },
    'ellissi.tsv': {
        'ellissi': {
            'pronunciation': 'eh-LEES-see',
            'translation_en': 'ellipsis (invariable)',
        },
    },
    'elmetto.tsv': {
        'elmetto': {
            'pronunciation': 'el-MEHT-toh',
            'translation_en': 'helmet',
        },
        'elmetti': {
            'pronunciation': 'el-MEHT-tee',
            'translation_en': 'helmets',
        },
    },
    'elmo.tsv': {
        'elmi': {
            'pronunciation': 'EL-mee',
            'translation_en': 'helms; helmets',
        },
    },
    'email.tsv': {
        'email': {
            'pronunciation': 'EE-mail',
            'translation_en': 'email',
        },
    },
    'emancipazione.tsv': {
        'emancipazione': {
            'pronunciation': 'eh-mahn-chee-pah-TSEE-oh-neh',
            'translation_en': 'emancipation',
        },
    },
    'emarginare.tsv': {
        'emarginato': {
            'pronunciation': 'eh-mahr-jee-NAH-toh',
            'translation_en': 'outcast (masculine)',
        },
    },
    'emarginata.tsv': {
        'emarginata': {
            'pronunciation': 'eh-mahr-jee-NAH-tah',
            'translation_en': 'outcast (feminine)',
        },
    },
    'embargo.tsv': {
        'embargo': {
            'pronunciation': 'ehm-BAR-goh',
            'translation_en': 'embargo',
        },
    },
    'emendamento.tsv': {
        'emendamento': {
            'pronunciation': 'eh-mehn-dah-MEN-toh',
            'translation_en': 'amendment',
        },
    },
    'emergenza.tsv': {
        'emergenza': {
            'pronunciation': 'eh-mehr-JEN-tsah',
            'translation_en': 'emergency',
        },
        'emergenze': {
            'pronunciation': 'eh-mehr-JEN-tseh',
            'translation_en': 'emergencies',
        },
    },
    'emigrante.tsv': {
        'emigranti': {
            'pronunciation': 'eh-mee-GRAHN-tee',
            'translation_en': 'emigrants',
        },
    },
    'emiro.tsv': {
        'emiro': {
            'pronunciation': 'eh-MEE-roh',
            'translation_en': 'emir',
        },
    },
    'emisfero.tsv': {
        'emisfero': {
            'pronunciation': 'eh-mees-FEH-roh',
            'translation_en': 'hemisphere',
        },
        'emisferi': {
            'pronunciation': 'eh-mees-FEH-ree',
            'translation_en': 'hemispheres',
        },
    },
    'emissione.tsv': {
        'emissioni': {
            'pronunciation': 'eh-mee-SSYOH-nee',
            'translation_en': 'emissions',
        },
        'emissione': {
            'pronunciation': 'eh-mee-SSYOH-neh',
            'translation_en': 'emission',
        },
    },
    'emittente.tsv': {
        'emittente': {
            'pronunciation': 'eh-meet-TEN-teh',
            'translation_en': 'broadcasting station; broadcaster',
        },
    },
    'emoglobina.tsv': {
        'emoglobina': {
            'pronunciation': 'eh-moh-gloh-BEE-nah',
            'translation_en': 'hemoglobin',
        },
    },
    'emorragia.tsv': {
        'emorragia': {
            'pronunciation': 'eh-moh-rah-JEE-ah',
            'translation_en': 'hemorrhage',
        },
    },
    'emorroido.tsv': {
        'emorroidi': {
            'pronunciation': 'eh-moh-ROY-dee',
            'translation_en': 'hemorrhoids',
        },
    },
    'emozione.tsv': {
        'emozioni': {
            'pronunciation': 'eh-moh-TSYOH-nee',
            'translation_en': 'emotions',
        },
        'emozione': {
            'pronunciation': 'eh-moh-TSYOH-neh',
            'translation_en': 'emotion',
        },
    },
    'empatia.tsv': {
        'empatia': {
            'pronunciation': 'ehm-pah-TEE-ah',
            'translation_en': 'empathy',
        },
    },
    'enciclopedia.tsv': {
        'enciclopedia': {
            'pronunciation': 'en-chee-kloh-peh-DEE-ah',
            'translation_en': 'encyclopedia',
        },
    },
    'endorfine.tsv': {
        'endorfine': {
            'pronunciation': 'en-dor-FEE-neh',
            'translation_en': 'endorphins',
        },
    },
    'energia.tsv': {
        'energia': {
            'pronunciation': 'eh-neh-REE-jah',
            'translation_en': 'energy',
        },
        'energie': {
            'pronunciation': 'eh-neh-REE-jeh',
            'translation_en': 'energies',
        },
    },
    'enfasi.tsv': {
        'enfasi': {
            'pronunciation': 'EN-fah-see',
            'translation_en': 'emphasis',
        },
    },
    'enigma.tsv': {
        'enigma': {
            'pronunciation': 'eh-NEEG-mah',
            'translation_en': 'enigma; riddle',
        },
    },
    'enigmio.tsv': {
        'enigmi': {
            'pronunciation': 'eh-NEEG-mee',
            'translation_en': 'enigmas; riddles',
        },
    },
    'enne.tsv': {
        'enne': {
            'pronunciation': 'EN-neh',
            'translation_en': 'year-old (suffix)',
        },
    },
    'ente.tsv': {
        'enti': {
            'pronunciation': 'EN-tee',
            'translation_en': 'entities; agencies',
        },
    },
    'entità.tsv': {
        'entità': {
            'pronunciation': 'en-tee-TAH',
            'translation_en': 'entity; extent',
        },
    },
    'entrata.tsv': {
        'entrata': {
            'pronunciation': 'en-TRAH-tah',
            'translation_en': 'entrance; entry',
        },
        'entrate': {
            'pronunciation': 'en-TRAH-teh',
            'translation_en': 'income; revenues (plural)',
        },
    },
    'entrate.tsv': {
        'entrate': {
            'pronunciation': 'en-TRAH-teh',
            'translation_en': 'entrances',
        },
    },
    'entropia.tsv': {
        'entropia': {
            'pronunciation': 'en-troh-PEE-ah',
            'translation_en': 'entropy',
        },
    },
    'entusiasmo.tsv': {
        'entusiasmo': {
            'pronunciation': 'en-too-zee-AHZ-moh',
            'translation_en': 'enthusiasm',
        },
    },
    'enumerazione.tsv': {
        'enumerazione': {
            'pronunciation': 'eh-noo-meh-rah-TSEE-oh-neh',
            'translation_en': 'enumeration; listing',
        },
    },
    'enunciato.tsv': {
        'enunciato': {
            'pronunciation': 'eh-noon-CHAH-toh',
            'translation_en': 'statement',
        },
    },
    'eosdigital.tsv': {
        'eosdigital': {
            'pronunciation': 'EH-ohs-DEE-jee-tahl',
            'translation_en': 'eosdigital (password)',
        },
    },
    'epidemia.tsv': {
        'epidemia': {
            'pronunciation': 'eh-pee-deh-MEE-ah',
            'translation_en': 'epidemic',
        },
    },
    'episodio.tsv': {
        'episodio': {
            'pronunciation': 'eh-pee-ZOH-dyoh',
            'translation_en': 'episode',
        },
    },
    'epoca.tsv': {
        'epoca': {
            'pronunciation': 'EH-poh-kah',
            'translation_en': 'era; epoch; period',
        },
    },
    'equatore.tsv': {
        'equatore': {
            'pronunciation': 'eh-kwah-TOH-reh',
            'translation_en': 'equator',
        },
    },
    'equazione.tsv': {
        'equazioni': {
            'pronunciation': 'eh-kwah-TSYOH-nee',
            'translation_en': 'equations',
        },
        'equazione': {
            'pronunciation': 'eh-kwah-TSYOH-neh',
            'translation_en': 'equation',
        },
    },
    'equidistanza.tsv': {
        'equidistanza': {
            'pronunciation': 'eh-kwee-dee-STAHN-tsah',
            'translation_en': 'equidistance',
        },
    },
    'equilibrio.tsv': {
        'equilibrio': {
            'pronunciation': 'eh-kwee-LEE-bryoh',
            'translation_en': 'balance; equilibrium',
        },
    },
    'equino.tsv': {
        'equino': {
            'pronunciation': 'eh-KWEE-noh',
            'translation_en': 'equine animal',
        },
    },
    'equipaggio.tsv': {
        'equipaggio': {
            'pronunciation': 'eh-kwee-PAH-joh',
            'translation_en': 'crew',
        },
    },
    'equivalente.tsv': {
        'equivalenti': {
            'pronunciation': 'eh-kwee-vah-LEN-tee',
            'translation_en': 'equivalents',
        },
        'equivalente': {
            'pronunciation': 'eh-kwee-vah-LEN-teh',
            'translation_en': 'equivalent',
        },
    },
    'equivoco.tsv': {
        'equivoco': {
            'pronunciation': 'eh-KWEE-voh-koh',
            'translation_en': 'misunderstanding; ambiguity',
        },
    },
    'era.tsv': {
        'era': {
            'pronunciation': 'EH-rah',
            'translation_en': 'era; epoch',
        },
        'ere': {
            'pronunciation': 'EH-reh',
            'translation_en': 'eras',
        },
    },
    'erba.tsv': {
        'erba': {
            'pronunciation': 'EHR-bah',
            'translation_en': 'grass; herb',
        },
    },
    'erbaccia.tsv': {
        'erbacce': {
            'pronunciation': 'ehr-BAH-cheh',
            'translation_en': 'weeds',
        },
    },
    'erede.tsv': {
        'erede': {
            'pronunciation': 'eh-REH-deh',
            'translation_en': 'heir',
        },
        'eredi': {
            'pronunciation': 'eh-REH-dee',
            'translation_en': 'heirs',
        },
    },
    'eredità.tsv': {
        'eredità': {
            'pronunciation': 'eh-reh-dee-TAH',
            'translation_en': 'inheritance; legacy',
        },
    },
    'eremita.tsv': {
        'eremita': {
            'pronunciation': 'eh-reh-MEE-tah',
            'translation_en': 'hermit',
        },
    },
    'eretica.tsv': {
        'eretica': {
            'pronunciation': 'eh-REH-tee-kah',
            'translation_en': 'heretic (feminine)',
        },
    },
    'erezione.tsv': {
        'erezione': {
            'pronunciation': 'eh-reh-TSYOH-neh',
            'translation_en': 'erection',
        },
    },
    'ergastolo.tsv': {
        'ergastolo': {
            'pronunciation': 'er-GAH-stoh-loh',
            'translation_en': 'life sentence; life imprisonment',
        },
    },
    'eritema.tsv': {
        'eritema': {
            'pronunciation': 'eh-ree-TEH-mah',
            'translation_en': 'rash; erythema',
        },
    },
    'eroe.tsv': {
        'eroe': {
            'pronunciation': 'eh-ROH-eh',
            'translation_en': 'hero',
        },
        'eroi': {
            'pronunciation': 'eh-ROH-ee',
            'translation_en': 'heroes',
        },
    },
    'eroina.tsv': {
        'eroina': {
            'pronunciation': 'eh-roh-EE-nah',
            'translation_en': 'heroin; heroine',
        },
    },
    'eroinomane.tsv': {
        'eroinomane': {
            'pronunciation': 'eh-roh-ee-NOH-mah-neh',
            'translation_en': 'heroin addict',
        },
    },
    'eroismo.tsv': {
        'eroismo': {
            'pronunciation': 'eh-roh-EEZ-moh',
            'translation_en': 'heroism',
        },
    },
    'erore.tsv': {
        'erore': {
            'pronunciation': 'eh-ROH-reh',
            'translation_en': 'mistake (variant spelling)',
        },
    },
    'erotismo.tsv': {
        'erotismo': {
            'pronunciation': 'eh-roh-TEEZ-moh',
            'translation_en': 'eroticism',
        },
    },
    'erpice.tsv': {
        'erpice': {
            'pronunciation': 'ER-pee-cheh',
            'translation_en': 'harrow',
        },
    },
    'errore.tsv': {
        'errore': {
            'pronunciation': 'eh-ROH-reh',
            'translation_en': 'error; mistake',
        },
        'errori': {
            'pronunciation': 'eh-ROH-ree',
            'translation_en': 'errors',
        },
    },
    'erta.tsv': {
        'erta': {
            'pronunciation': 'EHR-tah',
            'translation_en': 'steep slope; incline',
        },
    },
    'erudito.tsv': {
        'erudito': {
            'pronunciation': 'eh-roo-DEE-toh',
            'translation_en': 'scholar; learned man',
        },
    },
    'eruzione.tsv': {
        'eruzione': {
            'pronunciation': 'eh-roo-TSEE-oh-neh',
            'translation_en': 'eruption',
        },
    },
    'esagerazione.tsv': {
        'esagerazione': {
            'pronunciation': 'eh-zah-geh-rah-TSEE-oh-neh',
            'translation_en': 'exaggeration',
        },
    },
    'esagono.tsv': {
        'esagono': {
            'pronunciation': 'eh-ZAH-goh-noh',
            'translation_en': 'hexagon',
        },
    },
    'esame.tsv': {
        'esame': {
            'pronunciation': 'eh-ZAH-meh',
            'translation_en': 'exam',
        },
        'esami': {
            'pronunciation': 'eh-ZAH-mee',
            'translation_en': 'exams',
        },
    },
    'esaminare.tsv': {
        'esamini': {
            'pronunciation': 'eh-zah-MEE-nee',
            'translation_en': 'tests; little exams (plural)',
        },
    },
    'esattezza.tsv': {
        'esattezza': {
            'pronunciation': 'eh-zaht-TEHT-tsah',
            'translation_en': 'accuracy; exactness',
        },
    },
    'esca.tsv': {
        'esca': {
            'pronunciation': 'EH-skah',
            'translation_en': 'bait; lure',
        },
    },
    'escandescenza.tsv': {
        'escandescenza': {
            'pronunciation': 'eh-skahn-dehs-SHEN-tsah',
            'translation_en': 'outburst; tantrum',
        },
    },
    'eschimese.tsv': {
        'eschimesi': {
            'pronunciation': 'eh-skee-MEH-zee',
            'translation_en': 'Eskimos (dated)',
        },
    },
    'esclusione.tsv': {
        'esclusione': {
            'pronunciation': 'eh-skloo-ZYOH-neh',
            'translation_en': 'exclusion',
        },
    },
    'escursione.tsv': {
        'escursione': {
            'pronunciation': 'eh-sk oor-SYOH-neh',
            'translation_en': 'excursion; hike',
        },
        'escursioni': {
            'pronunciation': 'eh-sk oor-SYOH-nee',
            'translation_en': 'excursions; hikes',
        },
    },
    'escursionista.tsv': {
        'escursionisti': {
            'pronunciation': 'eh-sk oor-syo-NEES-tee',
            'translation_en': 'hikers; trekkers',
        },
    },
    'esecuzione.tsv': {
        'esecuzione': {
            'pronunciation': 'eh-zeh-koo-TSEE-oh-neh',
            'translation_en': 'execution; performance',
        },
    },
    'esempio.tsv': {
        'esempio': {
            'pronunciation': 'eh-ZEM-pyoh',
            'translation_en': 'example',
        },
        'esempi': {
            'pronunciation': 'eh-ZEM-pee',
            'translation_en': 'examples',
        },
    },
    'esemplare.tsv': {
        'esemplare': {
            'pronunciation': 'eh-zehm-PLAH-reh',
            'translation_en': 'specimen; copy',
        },
        'esemplari': {
            'pronunciation': 'eh-zehm-PLAH-ree',
            'translation_en': 'specimens; copies',
        },
    },
    'esercito.tsv': {
        'esercito': {
            'pronunciation': 'eh-ZEHR-chee-toh',
            'translation_en': 'army',
        },
    },
    'esercizio.tsv': {
        'esercizio': {
            'pronunciation': 'eh-zehr-CHEE-tsyoh',
            'translation_en': 'exercise; practice',
        },
        'esercizi': {
            'pronunciation': 'eh-zehr-CHEE-tsee',
            'translation_en': 'exercises',
        },
    },
    'esibizione.tsv': {
        'esibizione': {
            'pronunciation': 'eh-zee-bee-TSEE-oh-neh',
            'translation_en': 'performance; exhibition',
        },
    },
    'esigenza.tsv': {
        'esigenze': {
            'pronunciation': 'eh-zee-JEN-tseh',
            'translation_en': 'needs; requirements',
        },
        'esigenza': {
            'pronunciation': 'eh-zee-JEN-tsah',
            'translation_en': 'need; requirement',
        },
    },
    'esilio.tsv': {
        'esilio': {
            'pronunciation': 'eh-ZEE-lyoh',
            'translation_en': 'exile',
        },
    },
    'esistenza.tsv': {
        'esistenza': {
            'pronunciation': 'eh-zee-STEN-tsah',
            'translation_en': 'existence',
        },
    },
    'esitazione.tsv': {
        'esitazione': {
            'pronunciation': 'eh-zee-tah-TSEE-oh-neh',
            'translation_en': 'hesitation',
        },
    },
    'esito.tsv': {
        'esito': {
            'pronunciation': 'EH-zee-toh',
            'translation_en': 'outcome; result',
        },
    },
    'esordiente.tsv': {
        'esordiente': {
            'pronunciation': 'eh-zor-DYEN-teh',
            'translation_en': 'beginner; debutant',
        },
    },
    'espansione.tsv': {
        'espansione': {
            'pronunciation': 'eh-spahn-SYOH-neh',
            'translation_en': 'expansion',
        },
    },
    'esperantista.tsv': {
        'esperantisti': {
            'pronunciation': 'eh-speh-rahn-TEE-stee',
            'translation_en': 'Esperantists',
        },
        'esperantista': {
            'pronunciation': 'eh-speh-rahn-TEE-stah',
            'translation_en': 'Esperantist',
        },
    },
    'esperanto.tsv': {
        'esperanto': {
            'pronunciation': 'eh-speh-RAHN-toh',
            'translation_en': 'Esperanto',
        },
    },
    'esperienza.tsv': {
        'esperienza': {
            'pronunciation': 'eh-speh-ree-EHN-tsah',
            'translation_en': 'experience',
        },
        'esperienze': {
            'pronunciation': 'eh-speh-ree-EHN-tseh',
            'translation_en': 'experiences',
        },
    },
    'esperimento.tsv': {
        'esperimento': {
            'pronunciation': 'eh-speh-ree-MEN-toh',
            'translation_en': 'experiment',
        },
        'esperimenti': {
            'pronunciation': 'eh-speh-ree-MEN-tee',
            'translation_en': 'experiments',
        },
    },
    'esperto.tsv': {
        'esperto': {
            'pronunciation': 'eh-SPEHR-toh',
            'translation_en': 'expert (masculine)',
        },
        'esperti': {
            'pronunciation': 'eh-SPEHR-tee',
            'translation_en': 'experts (masculine)',
        },
        'esperta': {
            'pronunciation': 'eh-SPEHR-tah',
            'translation_en': 'expert (feminine)',
        },
    },
    'esploratore.tsv': {
        'esploratore': {
            'pronunciation': 'eh-sploh-rah-TOH-reh',
            'translation_en': 'explorer (masculine)',
        },
        'esploratori': {
            'pronunciation': 'eh-sploh-rah-TOH-ree',
            'translation_en': 'explorers (masculine)',
        },
        'esploratrice': {
            'pronunciation': 'eh-sploh-rah-TREE-cheh',
            'translation_en': 'explorer (feminine)',
        },
    },
    'esplosione.tsv': {
        'esplosione': {
            'pronunciation': 'eh-sploh-ZYOH-neh',
            'translation_en': 'explosion',
        },
        'esplosioni': {
            'pronunciation': 'eh-sploh-ZYOH-nee',
            'translation_en': 'explosions',
        },
    },
    'esplosivo.tsv': {
        'esplosivo': {
            'pronunciation': 'eh-sploh-ZEE-voh',
            'translation_en': 'explosive',
        },
    },
    'esportazione.tsv': {
        'esportazione': {
            'pronunciation': 'eh-spor-tah-TSEE-oh-neh',
            'translation_en': 'export; exportation',
        },
        'esportazioni': {
            'pronunciation': 'eh-spor-tah-TSEE-oh-nee',
            'translation_en': 'exports',
        },
    },
    'espositore.tsv': {
        'espositore': {
            'pronunciation': 'eh-spoh-zee-TOH-reh',
            'translation_en': 'exhibitor (masculine)',
        },
        'espositrice': {
            'pronunciation': 'eh-spoh-zee-TREE-cheh',
            'translation_en': 'exhibitor (feminine)',
        },
    },
    'esposizione.tsv': {
        'esposizione': {
            'pronunciation': 'eh-spoh-zee-TSEE-oh-neh',
            'translation_en': 'exhibition; exposure',
        },
    },
    'espressione.tsv': {
        'espressione': {
            'pronunciation': 'eh-spreh-SSYOH-neh',
            'translation_en': 'expression',
        },
        'espressioni': {
            'pronunciation': 'eh-spreh-SSYOH-nee',
            'translation_en': 'expressions',
        },
    },
    'espressività.tsv': {
        'espressività': {
            'pronunciation': 'eh-spreh-see-vee-TAH',
            'translation_en': 'expressiveness',
        },
    },
    'espresso.tsv': {
        'espresso': {
            'pronunciation': 'eh-SPREHS-soh',
            'translation_en': 'espresso; express train',
        },
    },
    'essenza.tsv': {
        'essenza': {
            'pronunciation': 'eh-SEHN-tsah',
            'translation_en': 'essence; extract',
        },
    },
    'essere.tsv': {
        'esseri': {
            'pronunciation': 'EH-seh-ree',
            'translation_en': 'beings',
        },
        'essere': {
            'pronunciation': 'EH-seh-reh',
            'translation_en': 'being',
        },
    },
    'esserto.tsv': {
        'esserti': {
            'pronunciation': 'EH-sehr-tee',
            'translation_en': 'yourself being (reflexive)',
        },
    },
    'est.tsv': {
        'est': {
            'pronunciation': 'EHST',
            'translation_en': 'east',
        },
    },
    'estasi.tsv': {
        'estasi': {
            'pronunciation': 'EH-stah-zee',
            'translation_en': 'ecstasy',
        },
    },
    'estate.tsv': {
        'estate': {
            'pronunciation': 'eh-STAH-teh',
            'translation_en': 'summer',
        },
    },
    'estato.tsv': {
        'estati': {
            'pronunciation': 'eh-STAH-tee',
            'translation_en': 'summers',
        },
    },
    'estensione.tsv': {
        'estensione': {
            'pronunciation': 'eh-stehn-SYOH-neh',
            'translation_en': 'extension; expanse',
        },
    },
    'esterno.tsv': {
        'esterno': {
            'pronunciation': 'eh-STEHR-noh',
            'translation_en': 'outside; exterior',
        },
    },
    'estero.tsv': {
        'estero': {
            'pronunciation': 'EH-steh-roh',
            'translation_en': 'foreign country; abroad',
        },
    },
    'estete.tsv': {
        'est.': {
            'pronunciation': 'EHST',
            'translation_en': 'east (abbreviation)',
        },
    },
    'estintore.tsv': {
        'estintore': {
            'pronunciation': 'eh-steen-TOH-reh',
            'translation_en': 'fire extinguisher',
        },
    },
    'estinzione.tsv': {
        'estinzione': {
            'pronunciation': 'eh-steen-TSEE-oh-neh',
            'translation_en': 'extinction',
        },
    },
    'estone.tsv': {
        'estone': {
            'pronunciation': 'eh-STOH-neh',
            'translation_en': 'Estonian (masculine)',
        },
    },
    'estorsione.tsv': {
        'estorsione': {
            'pronunciation': 'eh-stor-SYOH-neh',
            'translation_en': 'extortion',
        },
    },
    'estraneo.tsv': {
        'estranei': {
            'pronunciation': 'eh-STRAH-neh-ee',
            'translation_en': 'strangers (masculine)',
        },
        'estraneo': {
            'pronunciation': 'eh-STRAH-neh-oh',
            'translation_en': 'stranger (masculine)',
        },
        'estranea': {
            'pronunciation': 'eh-STRAH-neh-ah',
            'translation_en': 'stranger (feminine)',
        },
    },
    'estratto.tsv': {
        'estratto': {
            'pronunciation': 'eh-STRAHT-toh',
            'translation_en': 'extract; excerpt',
        },
    },
    'estrazione.tsv': {
        'estrazione': {
            'pronunciation': 'eh-strah-TSEE-oh-neh',
            'translation_en': 'extraction; drawing',
        },
    },
    'estremismo.tsv': {
        'estremismi': {
            'pronunciation': 'eh-streh-MEES-mee',
            'translation_en': 'extremisms',
        },
        'estremismo': {
            'pronunciation': 'eh-streh-MEES-moh',
            'translation_en': 'extremism',
        },
    },
    'estremista.tsv': {
        'estremisti': {
            'pronunciation': 'eh-streh-MEES-tee',
            'translation_en': 'extremists',
        },
    },
    'estremità.tsv': {
        'estremità': {
            'pronunciation': 'eh-streh-mee-TAH',
            'translation_en': 'end; extremity',
        },
    },
    'estremo.tsv': {
        'estremi': {
            'pronunciation': 'eh-STREH-mee',
            'translation_en': 'extremes',
        },
        'estremo': {
            'pronunciation': 'eh-STREH-moh',
            'translation_en': 'extreme; far end',
        },
    },
    'etanolo.tsv': {
        'etanolo': {
            'pronunciation': 'eh-tah-NOH-loh',
            'translation_en': 'ethanol',
        },
    },
    'eternità.tsv': {
        'eternità': {
            'pronunciation': 'eh-tehr-nee-TAH',
            'translation_en': 'eternity',
        },
    },
    'eterno.tsv': {
        'eterno': {
            'pronunciation': 'eh-TEHR-noh',
            'translation_en': 'eternal one; everlasting',
        },
    },
    'etica.tsv': {
        'etica': {
            'pronunciation': 'EH-tee-kah',
            'translation_en': 'ethics',
        },
    },
    'etichetta.tsv': {
        'etichette': {
            'pronunciation': 'eh-tee-KEHT-teh',
            'translation_en': 'labels; tags',
        },
        'etichetta': {
            'pronunciation': 'eh-tee-KEHT-tah',
            'translation_en': 'label; tag; etiquette',
        },
    },
    'etimologia.tsv': {
        'etimologia': {
            'pronunciation': 'eh-tee-moh-loh-JEE-ah',
            'translation_en': 'etymology',
        },
    },
    'etnia.tsv': {
        'etnie': {
            'pronunciation': 'eht-NEE-eh',
            'translation_en': 'ethnic groups',
        },
    },
    'ettolitro.tsv': {
        'ettolitro': {
            'pronunciation': 'et-TOH-lee-troh',
            'translation_en': 'hectoliter',
        },
    },
    'età.tsv': {
        'età': {
            'pronunciation': 'eh-TAH',
            'translation_en': 'age',
        },
    },
    'eucalipto.tsv': {
        'eucalipto': {
            'pronunciation': 'eh-oo-kah-LEEP-toh',
            'translation_en': 'eucalyptus',
        },
    },
    'euro.tsv': {
        'euro': {
            'pronunciation': 'EH-oo-roh',
            'translation_en': 'euro',
        },
    },
    'europeo.tsv': {
        'europei': {
            'pronunciation': 'eh-oo-roh-PEH-ee',
            'translation_en': 'Europeans (masculine)',
        },
        'europeo': {
            'pronunciation': 'eh-oo-roh-PEH-oh',
            'translation_en': 'European (masculine)',
        },
    },
    'eurozona.tsv': {
        'eurozona': {
            'pronunciation': 'eh-oo-ROH-dzoh-nah',
            'translation_en': 'eurozone',
        },
    },
    'evaporazione.tsv': {
        'evaporazione': {
            'pronunciation': 'eh-vah-poh-rah-TSEE-oh-neh',
            'translation_en': 'evaporation',
        },
    },
    'evento.tsv': {
        'evento': {
            'pronunciation': 'eh-VEHN-toh',
            'translation_en': 'event',
        },
        'eventi': {
            'pronunciation': 'eh-VEHN-tee',
            'translation_en': 'events',
        },
    },
    'evidenza.tsv': {
        'evidenza': {
            'pronunciation': 'eh-vee-DEN-tsah',
            'translation_en': 'evidence; prominence',
        },
    },
    'evoluzione.tsv': {
        'evoluzione': {
            'pronunciation': 'eh-voh-loo-TSEE-oh-neh',
            'translation_en': 'evolution; development',
        },
    },
}


def update_file(rel_path: str, entries: dict) -> None:
    path = os.path.join(BASE_DIR, rel_path)
    with open(path, newline='') as fh:
        rows = list(csv.DictReader(fh, delimiter='\t'))

    missing = set(entries)
    for row in rows:
        text = row['text']
        if text not in entries:
            continue
        info = entries[text]
        row['pronunciation'] = info['pronunciation']
        row['translation_en'] = info['translation_en']
        missing.discard(text)

    if missing:
        raise ValueError(f"Missing rows {sorted(missing)} in {rel_path}")

    fieldnames = rows[0].keys()
    with open(path, 'w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for rel_path, entries in UPDATES.items():
        update_file(rel_path, entries)


if __name__ == '__main__':
    main()
