import csv
from pathlib import Path

DATA = """text\tpronunciation\ttranslation
apatia\tah-pah-TEE-ah\tapathy
ape\tAH-peh\tbee
api\tAH-pee\tbees
aperto\tah-PEHR-toh\topen air; outdoors (masculine)
aperta\tah-PEHR-tah\topen air; outdoors (feminine)
apertura\tah-pehr-TOO-rah\topening; aperture
apice\tAH-pee-cheh\tapex; peak
apilado\tah-pee-LAH-doh\tclose-embrace tango style
apnea\tahp-NEH-ah\tapnea
apocalisse\tah-poh-kah-LEE-seh\tapocalypse
apostoli\tah-POH-stoh-lee\tapostles
apostolo\tah-POH-stoh-loh\tapostle
apostrofi\tah-POH-stroh-fee\tapostrophes
appagamento\tahp-pah-gah-MEN-toh\tsatisfaction; fulfillment
apparecchiatura\tahp-pah-rehk-kyah-TOO-rah\tequipment; apparatus
apparecchio\tahp-pah-REK-kyoh\tdevice; appliance
apparenza\tahp-pah-REN-zah\tappearance; outward look
apparenze\tahp-pah-REN-zeh\tappearances; outward looks
apparizione\tahp-pah-ree-TZYOH-neh\tapparition; appearance
appartenenza\tahp-par-teh-NEN-zah\tmembership; belonging
appassionato\tahp-pahs-syoh-NAH-toh\tenthusiast (masculine)
appassionati\tahp-pahs-syoh-NAH-tee\tenthusiasts (masculine)
appassionata\tahp-pahs-syoh-NAH-tah\tenthusiast (feminine)
appendice\tahp-pehn-DEE-cheh\tappendix
appendicite\tahp-pehn-dee-CHEE-teh\tappendicitis
appetito\tahp-peh-TEE-toh\tappetite
applausi\tahp-PLOW-zee\trounds of applause
applauso\tahp-PLOW-zoh\tapplause; round of applause
applicazione\tahp-plee-kah-TZYOH-neh\tapplication; app
applicazioni\tahp-plee-kah-TZYOH-nee\tapplications; apps
appoggio\tahp-POJ-joh\tsupport; backing
appoggi\tahp-POJ-jee\tsupports; backings
apprendimento\tahp-pren-dee-MEN-toh\tlearning
apprendista\tahp-pren-DEE-stah\tapprentice
apprendistato\tahp-pren-dee-STAH-toh\tapprenticeship
apprezzamento\tahp-prehts-tsah-MEN-toh\tappreciation; esteem
approccio\tahp-PROH-choh\tapproach
approvazione\tahp-proh-vah-TZYOH-neh\tapproval
approvvigionamento\tahp-prohv-vee-joh-nah-MEN-toh\tsupply; provisioning
appuntamento\tahp-poon-tah-MEN-toh\tappointment; date
appunti\tahp-POON-tee\tnotes
appunto\tahp-POON-toh\tnote; point
apri-scatola\tAH-pree-SKAH-toh-lah\tcan opener
aprile\tah-PREE-leh\tapril
aprilo\tAH-pree-loh\topen it
apriscatole\tAH-pree-SKAH-toh-leh\tcan opener
aquila\tAH-kwee-lah\teagle
aquilone\tah-kwee-LOH-neh\tkite
aquiloni\tah-kwee-LOH-nee\tkites
arabesco\tah-rah-BEH-skoh\tarabesque
arabo\tAH-rah-boh\tArab (masculine)
arabi\tAH-rah-bee\tArabs (masculine)
arachidi\tah-RAH-kee-dee\tpeanuts
aracnidi\tah-RAHK-nee-dee\tarachnids
aragoste\tah-rah-GOH-steh\tlobsters
aragosta\tah-rah-GOH-stah\tlobster
aramaico\tah-rah-MAH-ee-koh\tAramaic
arancia\tah-RAHN-chah\torange
arance\tah-RAHN-cheh\toranges
aranciata\tah-RAHN-chah-TAH\torange soda
arancio\tah-RAHN-chyoh\torange tree
arbitro\tAHR-bee-troh\treferee
arca\tAHR-kah\tark
arcano\tahr-KAH-noh\tmystery; enigma
archeologia\tahr-keh-oh-loh-JEE-ah\tarchaeology
architetto\tahr-kee-TET-toh\tarchitect
architettura\tahr-kee-teht-TOO-rah\tarchitecture
archivio\tahr-KEE-vyoh\tarchive
archivi\tahr-KEE-vee\tarchives
arciere\tahr-CHEH-reh\tarcher
arcivescovo\tahr-chee-VEHS-koh-voh\tarchbishop
arco\tAHR-koh\tarch; bow
arcobaleno\tahr-koh-bah-LEH-noh\trainbow
arcobaleni\tahr-koh-bah-LEH-nee\trainbows
area\tAH-reh-ah\tarea; zone
aree\tAH-reh-eh\tareas; zones
argentini\tahr-jen-TEE-nee\tArgentines (masculine)
argentino\tahr-jen-TEE-noh\tArgentine (masculine)
argento\tahr-JEN-toh\tsilver
argilla\tahr-JEEL-lah\tclay
argine\tAHR-jee-neh\tembankment; levee
argini\tAHR-jee-nee\tembankments; levees
argomentazione\tahr-goh-men-tah-TZYOH-neh\targumentation; reasoning
argomento\tahr-goh-MEN-toh\ttopic; argument
argomenti\tahr-goh-MEN-tee\ttopics; arguments
argon\tAHR-gon\targon
aristocratica\tah-ree-stoh-KRAH-tee-kah\taristocrat (feminine)
aristocratici\tah-ree-stoh-KRAH-tee-chee\taristocrats (masculine)
aristocratico\tah-ree-stoh-KRAH-tee-koh\taristocrat (masculine)
aristocrazia\tah-ree-stoh-krah-TSEE-ah\taristocracy
aritmetica\tah-reet-MEH-tee-kah\tarithmetic
armi\tAHR-mee\tweapons
arma\tAHR-mah\tweapon
armadietto\tahr-mah-DYET-toh\tlocker; small cabinet
armadio\tahr-MAH-dyoh\twardrobe; closet
armeno\tahr-MEH-noh\tArmenian (masculine)
armonia\tahr-moh-NEE-ah\tharmony
aroma\tah-ROH-mah\taroma
aromaterapia\tah-roh-mah-teh-rah-PEE-ah\taromatherapy
arpa\tAHR-pah\tharp
arrampicata\tahr-rahm-pee-KAH-tah\tclimbing
arredamento\tahr-reh-dah-MEN-toh\tfurnishing; furniture
arredo\tahr-REH-doh\tfurnishings; décor
arresto\tahr-REH-stoh\tarrest
arretrato\tahr-reh-TRAH-toh\tarrears; backlog
arrivato\tahr-ree-VAH-toh\tnewcomer
arrivo\tahr-REE-voh\tarrival
arroganza\tahr-roh-GAN-zah\tarrogance
arrosto\tahr-ROH-stoh\troast; roasted meat
arrotino\tahr-roh-TEE-noh\tknife grinder
arsenale\tahr-seh-NAH-leh\tarsenal; shipyard
arterie\tahr-TEH-ryeh\tarteries
arteriole\tahr-teh-REE-oh-leh\tarterioles
articolazione\tahr-tee-koh-lah-TZYOH-neh\tjoint; articulation
articolazioni\tahr-tee-koh-lah-TZYOH-nee\tjoints; articulations
artificio\tahr-tee-FEE-chyoh\tartifice; trick
artigli\tahr-TEE-lyee\tclaws
artista\tahr-TEE-stah\tartist
artisti\tahr-TEE-stee\tartists (masculine)
artiste\tahr-TEE-steh\tartists (feminine)
artrite\tahr-TREE-teh\tarthritis
ascensore\tah-shen-SOH-reh\televator
ascensori\tah-shen-SOH-ree\televators
ascesa\tah-SHEH-zah\tascent; rise
ascia\tAH-shah\taxe
asce\tAH-sheh\taxes
asciugacapelli\tah-SHOO-gah-kah-PEL-lee\thair dryer
asciugamano\tah-SHOO-gah-MAH-noh\ttowel
asciugamani\tah-SHOO-gah-MAH-nee\ttowels
ascolto\tahs-KOHL-toh\tlistening; heed
ascoltatore\tahs-kohl-tah-TOH-reh\tlistener (masculine)
ascoltatori\tahs-kohl-tah-TOH-ree\tlisteners (masculine)
ascolti\tahs-KOHL-tee\tlisten (formal imperative)
asfissia\tahs-fee-SEE-ah\tasphyxia
asiatici\tah-zya-TEE-chee\tAsians (masculine)
asilo\tah-ZEE-loh\tkindergarten; daycare
asili\tah-ZEE-lee\tkindergartens; daycares
asino\tAH-zee-noh\tdonkey
asini\tAH-zee-nee\tdonkeys
asma\tAHZ-mah\tasthma
asparagi\tahs-PAH-rah-jee\tasparagus spears
aspartame\tahs-par-TAH-meh\taspartame
aspettative\tahs-peht-tah-TEE-veh\texpectations
aspettativa\tahs-peht-tah-TEE-vah\texpectation; leave of absence
aspirapolvere\tahs-pee-rah-POHL-veh-reh\tvacuum cleaner
aspirazioni\tahs-pee-rah-TZYOH-nee\taspirations
aspirina\tahs-pee-REE-nah\taspirin
aspirine\tahs-pee-REE-neh\taspirins
assaggi\tahs-SAHJ-jee\ttastings; samples
assaggio\tahs-SAHJ-joh\ttaste; sample
assale\tahs-SAH-leh\taxle
assassina\tahs-sahs-SEE-nah\tmurderer (feminine)
assassinio\tahs-sahs-SEE-nyoh\tmurder
assassino\tahs-sahs-SEE-noh\tmurderer (masculine)
assassini\tahs-sahs-SEE-nee\tmurderers (masculine)
asse\tAH-seh\tplank; axis
assedio\tahs-SEH-dyoh\tsiege
assegno\tahs-SEH-nyoh\tcheck; allowance
assegni\tahs-SEH-nyee\tchecks; allowances
assemblai\tahs-sem-BLAH-ee\tI assembled
assenti\tah-SEN-tee\tabsent people
assenza\tahs-SEHN-zah\tabsence
assenze\tahs-SEHN-zeh\tabsences
asserzione\tahs-sehr-TZYOH-neh\tassertion
assicurazione\tahs-see-koo-rah-TZYOH-neh\tinsurance
assicurazioni\tahs-see-koo-rah-TZYOH-nee\tinsurance policies
assioma\tahs-SYOH-mah\taxiom
assistente\tahs-see-STEN-teh\tassistant
assistenti\tahs-see-STEN-tee\tassistants
assistenza\tahs-see-STEN-zah\tassistance; care
asso\tAH-soh\tace
assi\tAH-see\taces
associazione\tahs-soh-chah-TZYOH-neh\tassociation
assoluto\tahs-soh-LOO-toh\tabsolute
assorbenti\tahs-sohr-BEN-tee\tabsorbent pads
assunto\tahs-SOON-toh\tnew hire; subject
assurdità\tahs-soor-dee-TAH\tabsurdity
astinenza\tahs-tee-NEN-zah\tabstinence
astrofisico\tahs-troh-FEE-zee-koh\tastrophysicist
astrologia\tahs-troh-loh-JEE-ah\tastrology
astronauta\tahs-troh-NOW-tah\tastronaut
astronauti\tahs-troh-NOW-tee\tastronauts (masculine)
astronaute\tahs-troh-NOW-teh\tastronauts (feminine)
astronomia\tahs-troh-noh-MEE-ah\tastronomy
astronomo\tahs-TROH-noh-moh\tastronomer
astuzia\tahs-TOO-tsyah\tcunning; craftiness
ateismo\tah-teh-EEZ-moh\tatheism
ateo\tAH-teh-oh\tatheist (masculine)
atleti\tah-TLEH-tee\tathletes (masculine)
atleta\tah-TLEH-tah\tathlete
atletica\tah-TLEH-tee-kah\ttrack and field; athletics
atmosfera\tahht-mohs-FEH-rah\tatmosphere
atomi\tAH-toh-mee\tatoms
atomo\tAH-toh-moh\tatom
atrio\tAH-tree-oh\tatrium
atrocità\tah-troh-chee-TAH\tatrocity
attacco\tah-TAHK-koh\tattack
attacchi\tah-TAHK-kee\tattacks
atteggiamento\tah-tehj-jah-MEN-toh\tattitude
atterraggio\tah-tehr-RAHJ-joh\tlanding
attesa\tah-TEH-zah\twait; expectation
attico\tAH-tee-koh\tpenthouse; attic apartment
attimo\tAH-tee-moh\tmoment
attinio\tah-TEE-nyoh\tactinium
attivista\tah-tee-VEE-stah\tactivist
attività\tah-tee-vee-TAH\tactivity
attivo\tah-TEE-voh\tsurplus; profit
atto\tAH-toh\tact; deed
atti\tAH-tee\tacts; deeds
attrattiva\tah-trah-TEE-vah\tappeal; attraction
attrazioni\tah-trah-TZYOH-nee\tattractions
attrazione\tah-trah-TZYOH-neh\tattraction
attrezzatura\tah-tret-tsah-TOO-rah\tequipment
attrezzi\tah-TRET-tsee\ttools
audaci\tow-DAH-chee\tbold people
audacia\tow-DAH-chah\tboldness
audio\tow-DYOH\taudio
audiocassetta\tow-dyoh-kahs-SET-tah\taudiocassette
audioguide\tow-dyoh-GWEE-deh\taudio guides
audiolibri\tow-dyoh-LEE-bree\taudiobooks
audiolibro\tow-dyoh-LEE-broh\taudiobook
auditorium\tow-dee-TOH-ryoom\tauditorium
auguri\tow-GOO-ree\tbest wishes
aula\tOW-lah\tclassroom
aumento\tow-MEN-toh\tincrease; raise
aure\tOW-reh\tauras
auricolare\tow-ree-koh-LAH-reh\tearpiece; earbud
aurora\tow-ROH-rah\tdawn; aurora
austerity\tow-steh-REE-tee\tausterity
austerità\tow-steh-ree-TAH\tausterity
australiano\tow-strah-lya-NAH-noh\tAustralian (masculine)
australiani\tow-strah-lya-NAH-nee\tAustralians (masculine)
autenticità\tow-ten-tee-chee-TAH\tauthenticity
autista\tow-TEE-stah\tdriver
autisti\tow-TEE-stee\tdrivers (masculine)
autocritica\tow-toh-KREE-tee-kah\tself-criticism
autodidatta\tow-toh-dee-DAHT-tah\tself-taught person
autodifesa\tow-toh-dee-FEH-zah\tself-defense
autografo\tow-TOH-grah-foh\tautograph
autografi\tow-TOH-grah-fee\tautographs
autonomia\tow-toh-noh-MEE-ah\tautonomy
autore\tow-TOH-reh\tauthor (masculine)
autori\tow-TOH-ree\tauthors (masculine)
autrice\tow-TREE-cheh\tauthor (feminine)
autorità\tow-toh-ree-TAH\tauthority
autorizzazione\tow-toh-reet-tsah-TZYOH-neh\tauthorization
autostima\tow-toh-STEE-mah\tself-esteem
autostop\tow-toh-STOHP\thitchhiking
autostrada\tow-toh-STRAH-dah\thighway
autovelox\tow-toh-VEH-lohks\tspeed camera
autunno\tow-TOON-noh\tautumn; fall
avambraccio\tah-vahm-BRAH-choh\tforearm
avanguardia\tah-vahn-GWAHR-dyah\tvanguard; avant-garde
avanzamenti\tah-vahn-tsah-MEN-tee\tadvancements
avanzamento\tah-vahn-tsah-MEN-toh\tadvancement
avanzata\tah-vahn-ZAH-tah\tadvance; offensive
avanzi\tah-VAHN-tsee\tleftovers
averi\tah-VEH-ree\tworldly goods
avidità\tah-vee-dee-TAH\tgreed
avocado\tah-voh-KAH-doh\tavocado
avvelenamento\tahv-veh-leh-nah-MEN-toh\tpoisoning
avvenimento\tahv-veh-nee-MEN-toh\tevent
avvenimenti\tahv-veh-nee-MEN-tee\tevents
avvenire\tahv-veh-NEE-reh\tfuture
avvento\tahv-VEHN-toh\tAdvent
avventura\tahv-vehn-TOO-rah\tadventure
avventure\tahv-vehn-TOO-reh\tadventures
avverbi\tahv-VEHR-bee\tadverbs
avverbio\tahv-VEHR-byoh\tadverb
avversario\tahv-vehr-SAHR-yoh\topponent (masculine)
avversari\tahv-vehr-SAHR-ee\topponents (masculine)
avversità\tahv-vehr-see-TAH\tadversity
avvertimento\tahv-vehr-tee-MEN-toh\twarning
avvertimenti\tahv-vehr-tee-MEN-tee\twarnings
avviamento\tahv-vyah-MEN-toh\tstart-up; ignition
avvio\tahv-VEE-oh\tstart; kickoff
avviso\tahv-VEE-zoh\tnotice; warning
avvisi\tahv-VEE-zee\tnotices; warnings
avvocatessa\tahv-voh-kah-TES-sah\tlawyer (feminine)
avvocatesse\tahv-voh-kah-TES-seh\tlawyers (feminine)
avvocato\tahv-voh-KAH-toh\tlawyer (masculine)
avvocati\tahv-voh-KAH-tee\tlawyers (masculine)
azalee\tah-zah-LEH-eh\tazaleas
azero\tah-ZEH-roh\tAzeri (masculine)
azienda\tahd-ZYEN-dah\tcompany
aziende\tahd-ZYEN-deh\tcompanies
azioni\tahd-ZYOH-nee\tstocks; actions
azione\tahd-ZYOH-neh\taction; stock
azoto\tah-ZOH-toh\tnitrogen
azzardo\tahz-ZAHR-doh\tgambling; risk
azzurro\tahz-ZOOR-roh\tblue (masculine)
"""

mapping = {}
for line in DATA.strip().splitlines():
    text, pron, trans = line.split('\t')
    mapping[text] = (pron, trans)

base = Path('vocabulary/dataframes/NOUN')
target_files = sorted([p for p in base.glob('*.tsv') if 'apatia.tsv' <= p.name <= 'azzurro.tsv'])

missing = []
for path in target_files:
    rows = list(csv.DictReader(path.open(), delimiter='\t'))
    header = rows[0].keys()
    changed = False
    for row in rows:
        text = row['text']
        if not row['translation_en'] or not row['pronunciation']:
            if text not in mapping:
                missing.append(text)
                continue
            pron, trans = mapping[text]
            row['pronunciation'] = pron
            row['translation_en'] = trans
            changed = True
    if changed:
        with path.open('w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=header, delimiter='\t', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)

if missing:
    raise SystemExit(f'Missing mappings for: {sorted(set(missing))}')
