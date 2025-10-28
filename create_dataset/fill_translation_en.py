#!/usr/bin/env python3
import csv
import os

TRANSLATIONS = {
    'create_dataset/dataframes/ADJ/altro.tsv': {
        'altra': 'other; another (feminine)',
        'altro': 'other; another (masculine)',
        'altre': 'other (feminine plural)',
        'altri': 'other (masculine plural)',
        "altr'": 'other; another (masculine, before a vowel)',
    },
    'create_dataset/dataframes/ADJ/bello.tsv': {
        'bella': 'beautiful (feminine)',
        'bello': 'handsome; beautiful; nice (masculine)',
        'bel': 'beautiful; nice (masculine, short form)',
        'belle': 'beautiful (feminine plural)',
        "bell'": 'good-looking; nice-looking (masculine, before a vowel)',
        'belli': 'handsome; beautiful; nice (masculine plural)',
        'bei': 'beautiful; nice (masculine plural, short form)',
        'bellissimo': 'very beautiful; gorgeous (masculine)',
        'bellissima': 'very beautiful; gorgeous (feminine)',
        'bellissimi': 'very beautiful; gorgeous (masculine plural)',
    },
    'create_dataset/dataframes/ADJ/buono.tsv': {
        'buono': 'good; kind; tasty (masculine)',
        'buona': 'good; kind; tasty (feminine)',
        'buoni': 'good; kind; tasty (masculine plural)',
        'buone': 'good; kind; tasty (feminine plural)',
        'buon': 'good; kind; tasty (masculine, short form)',
        "buon'": 'good; kind; tasty (masculine, short form, before a vowel)',
        'buonissimo': 'very good; delicious (masculine)',
    },
    'create_dataset/dataframes/ADJ/felice.tsv': {
        'felice': 'happy',
        'felici': 'happy (plural)',
        'felicissimo': 'very happy; overjoyed (masculine)',
    },
    'create_dataset/dataframes/ADJ/grande.tsv': {
        'grande': 'big; great',
        'grandi': 'big; great (plural)',
        'gran': 'great; grand (short form)',
        'grandissimo': 'very big; huge; tremendous (masculine)',
        'grandissime': 'very big; huge; tremendous (feminine plural)',
    },
    'create_dataset/dataframes/ADJ/nuovo.tsv': {
        'nuovo': 'new (masculine)',
        'nuova': 'new (feminine)',
        'nuovi': 'new (masculine plural)',
        'nuove': 'new (feminine plural)',
    },
    'create_dataset/dataframes/ADJ/primo.tsv': {
        'primo': 'first (masculine)',
        'prima': 'first (feminine)',
        'primi': 'first (masculine plural)',
        'prime': 'first (feminine plural)',
    },
    'create_dataset/dataframes/ADJ/scorso.tsv': {
        'scorso': 'last; past (masculine)',
        'scorsa': 'last; past (feminine)',
        'scorsi': 'last; past (masculine plural)',
    },
    'create_dataset/dataframes/ADJ/sicuro.tsv': {
        'sicuro': 'sure; certain; safe (masculine)',
        'sicura': 'sure; certain; safe (feminine)',
        'sicuri': 'sure; certain; safe (masculine plural)',
        'sicure': 'sure; certain; safe (feminine plural)',
        'sicurissimo': 'absolutely certain; sure (masculine)',
        'sicurissima': 'absolutely certain; sure (feminine)',
    },
    'create_dataset/dataframes/ADJ/solo.tsv': {
        'solo': 'alone; only (masculine)',
        'sola': 'alone; only (feminine)',
        'soli': 'alone; only (masculine plural)',
        'sole': 'only; sole (feminine plural)',
    },
    'create_dataset/dataframes/ADJ/stanco.tsv': {
        'stanco': 'tired (masculine)',
        'stanca': 'tired (feminine)',
        'stanchi': 'tired (masculine plural)',
        'stanche': 'tired (feminine plural)',
    },
    'create_dataset/dataframes/ADJ/vero.tsv': {
        'vero': 'true; real (masculine)',
        'vera': 'true; real (feminine)',
        'veri': 'true; real (masculine plural)',
        'vere': 'true; real (feminine plural)',
        'verissimo': 'very true (masculine)',
    },
    'create_dataset/dataframes/ADP/a.tsv': {
        'a': 'to; at; in',
        'ad': 'to; at; in',
        'al': 'to the; at the (masculine)',
        'alla': 'to the; at the (feminine)',
        'alle': 'to the; at the (feminine plural)',
        'ai': 'to the; at the (masculine plural)',
        'allo': 'to the; at the (masculine)',
        'agli': 'to the; at the (masculine plural)',
        "all'": 'to the; at the (before a vowel)',
        "al-'": 'to the; at the (before a vowel)',
    },
    'create_dataset/dataframes/ADP/come.tsv': {
        'come': 'like; as',
    },
    'create_dataset/dataframes/ADP/con.tsv': {
        'con': 'with; along with',
        'col': 'with the; along with the (masculine)',
    },
    'create_dataset/dataframes/ADP/da.tsv': {
        'da': 'from; by; since',
        'dal': 'from the; by the; since the (masculine)',
        'dallo': 'from the; by the; since the (masculine)',
        'dalla': 'from the; by the; since the (feminine)',
        'dalle': 'from the; by the; since the (feminine plural)',
        'dai': 'from the; by the; since the (masculine plural)',
        'dagli': 'from the; by the; since the (masculine plural)',
        "dall'": 'from the; by the; since the (before a vowel)',
        'dall': 'from the; by the; since the',
        "d'": 'from; by; since (before a vowel)',
        'dagliela': 'give it to her',
        'dategli': 'give it to him',
        'day': 'day',
    },
    'create_dataset/dataframes/ADP/di.tsv': {
        'di': 'of; from; about',
        'del': 'of the; from the; about the (masculine)',
        'dello': 'of the; from the; about the (masculine)',
        'della': 'of the; from the; about the (feminine)',
        'delle': 'of the; from the; about the (feminine plural)',
        'dei': 'of the; from the; about the (masculine plural)',
        'degli': 'of the; from the; about the (masculine plural)',
        "dell'": 'of the; from the; about the (before a vowel)',
        'dell': 'of the; from the; about the',
        "del'": 'of the; from the; about the (before a vowel)',
        "d'": 'of; from; about (before a vowel)',
        'dille': 'tell her',
        'digli': 'tell him',
        'ditegli': 'tell him',
        'diglielo': 'tell him it',
    },
    'create_dataset/dataframes/ADP/in.tsv': {
        'in': 'in; into; at',
        'nel': 'in the; into the; at the (masculine)',
        'nello': 'in the; into the; at the (masculine)',
        'nella': 'in the; into the; at the (feminine)',
        'nelle': 'in the; into the; at the (feminine plural)',
        'nei': 'in the; into the; at the (masculine plural)',
        'negli': 'in the; into the; at the (masculine plural)',
        "nell'": 'in the; into the; at the (before a vowel)',
        'hello': 'hello',
        'lei': 'she; you (formal)',
        'snello': 'slim; slender (masculine)',
    },
    'create_dataset/dataframes/ADP/per.tsv': {
        'per': 'for; through; in order to',
    },
    'create_dataset/dataframes/ADP/senza.tsv': {
        'senza': 'without',
        "senz'": 'without (before a vowel)',
    },
    'create_dataset/dataframes/ADP/su.tsv': {
        'su': 'on; over; about',
        'sul': 'on the; over the; about the (masculine)',
        'sullo': 'on the; over the; about the (masculine)',
        'sulla': 'on the; over the; about the (feminine)',
        'sulle': 'on the; over the; about the (feminine plural)',
        'sui': 'on the; over the; about the (masculine plural)',
        'sugli': 'on the; over the; about the (masculine plural)',
        "sull'": 'on the; over the; about the (before a vowel)',
    },
    'create_dataset/dataframes/CCONJ/e.tsv': {
        'e': 'and',
        'ed': 'and (before a vowel)',
        'ella': 'she',
    },
    'create_dataset/dataframes/CCONJ/ma.tsv': {
        'ma': 'but',
        'mal': 'evil; misfortune (masculine)',
    },
    'create_dataset/dataframes/CCONJ/o.tsv': {
        'o': 'or',
        'od': 'or (before a vowel)',
    },
    'create_dataset/dataframes/DET/alcuno.tsv': {
        'alcuno': 'any; some (masculine)',
        'alcuna': 'any; some (feminine)',
        'alcuni': 'some (masculine plural)',
        'alcune': 'some (feminine plural)',
        'alcun': 'any; some (masculine, short form)',
    },
    'create_dataset/dataframes/DET/che.tsv': {
        'che': 'what',
    },
    'create_dataset/dataframes/DET/il.tsv': {
        'il': 'the (masculine)',
        'lo': 'the (masculine) (before s+consonant)',
        'la': 'the (feminine)',
        "l'": 'the (before a vowel)',
        'l-': 'the',
        'i': 'the (masculine plural)',
        'gli': 'the (masculine plural)',
        'le': 'the (feminine plural)',
        'sei': 'are (second person)',
        'stella': 'star',
        'tel': 'Tel',
    },
    'create_dataset/dataframes/DET/mio.tsv': {
        'mio': 'my (masculine)',
        'mia': 'my (feminine)',
        'miei': 'my (masculine plural)',
        'mie': 'my (feminine plural)',
    },
    'create_dataset/dataframes/DET/molto.tsv': {
        'molto': 'a lot of; much (masculine)',
        'molta': 'a lot of; much (feminine)',
        'molti': 'many; a lot of (masculine plural)',
        'molte': 'many; a lot of (feminine plural)',
        'moltissimo': 'a great deal of; very much (masculine)',
        'moltissima': 'a great deal of; very much (feminine)',
        'moltissime': 'very many; a great many (feminine plural)',
    },
    'create_dataset/dataframes/DET/nostro.tsv': {
        'nostro': 'our (masculine)',
        'nostra': 'our (feminine)',
        'nostri': 'our (masculine plural)',
        'nostre': 'our (feminine plural)',
    },
    'create_dataset/dataframes/DET/ogni.tsv': {
        'ogni': 'every; each',
    },
    'create_dataset/dataframes/DET/qualche.tsv': {
        'qualche': 'some; a few',
    },
    'create_dataset/dataframes/DET/quanto.tsv': {
        'quanto': 'how much (masculine)',
        'quanta': 'how much (feminine)',
        'quanti': 'how many (masculine plural)',
        'quante': 'how many (feminine plural)',
        "quant'": 'whatever else (before a vowel)',
    },
    'create_dataset/dataframes/DET/quello.tsv': {
        'quello': 'that (masculine)',
        'quel': 'that (masculine, short form)',
        "quell'": 'that (before a vowel)',
        'quella': 'that (feminine)',
        'quelle': 'those (feminine plural)',
        'quelli': 'those (masculine plural)',
        'quegli': 'those (masculine plural)',
        'quei': 'those (masculine plural, short form)',
    },
    'create_dataset/dataframes/DET/questo.tsv': {
        'questo': 'this (masculine)',
        'questa': 'this (feminine)',
        'questi': 'these (masculine plural)',
        'queste': 'these (feminine plural)',
        "quest'": 'this (before a vowel)',
    },
    'create_dataset/dataframes/DET/suo.tsv': {
        'suo': 'his; her (masculine)',
        'sua': 'his; her (feminine)',
        'suoi': 'his; her (masculine plural)',
        'sue': 'his; her (feminine plural)',
    },
    'create_dataset/dataframes/DET/tuo.tsv': {
        'tuo': 'your (masculine)',
        'tua': 'your (feminine)',
        'tuoi': 'your (masculine plural)',
        'tue': 'your (feminine plural)',
    },
    'create_dataset/dataframes/DET/tutto.tsv': {
        'tutto': 'all the (masculine)',
        'tutta': 'all the (feminine)',
        'tutti': 'all the (masculine plural)',
        'tutte': 'all the (feminine plural)',
    },
    'create_dataset/dataframes/DET/uno.tsv': {
        'uno': 'a; one (masculine)',
        'un': 'a; one (masculine)',
        'una': 'a; one (feminine)',
        "un'": 'an; one (feminine) (before a vowel)',
    },
    'create_dataset/dataframes/DET/vostro.tsv': {
        'vostro': 'your (masculine, plural possessor)',
        'vostra': 'your (feminine, plural possessor)',
        'vostri': 'your (masculine plural, plural possessor)',
        'vostre': 'your (feminine plural, plural possessor)',
    },
    'create_dataset/dataframes/NUM/due.tsv': {
        'due': 'two',
    },
    'create_dataset/dataframes/NUM/tre.tsv': {
        'tre': 'three',
    },
    'create_dataset/dataframes/PRON/altro.tsv': {
        'altro': 'something else; anything else',
        'altri': 'others (masculine)',
        'altra': 'another one (feminine)',
        'altre': 'others (feminine plural)',
    },
    'create_dataset/dataframes/PRON/che.tsv': {
        'che': 'who; that; which',
    },
    'create_dataset/dataframes/PRON/chi.tsv': {
        'chi': 'who; whom',
    },
    'create_dataset/dataframes/PRON/ci.tsv': {
        'ci': 'us; to us; there',
        "c'": 'us; to us; there (before a vowel)',
    },
    'create_dataset/dataframes/PRON/ciò.tsv': {
        'ciò': 'that; this (neuter)',
    },
    'create_dataset/dataframes/PRON/cosa.tsv': {
        'cosa': 'what',
        "cos'": 'what (before a vowel)',
    },
    'create_dataset/dataframes/PRON/cui.tsv': {
        'cui': 'whom; which; to whom',
    },
    'create_dataset/dataframes/PRON/gli.tsv': {
        'gli': 'to him; to her; to it (masculine)',
    },
    'create_dataset/dataframes/PRON/io.tsv': {
        'io': 'I',
    },
    'create_dataset/dataframes/PRON/la.tsv': {
        'la': 'her; it (feminine)',
    },
    'create_dataset/dataframes/PRON/le.tsv': {
        'le': 'to her; to you (formal); to them (feminine)',
    },
    'create_dataset/dataframes/PRON/lei.tsv': {
        'lei': 'she; you (formal)',
    },
    'create_dataset/dataframes/PRON/lo.tsv': {
        'lo': 'him; it (masculine)',
        "l'": 'him; it (masculine, before a vowel)',
        'qual': 'which',
    },
    'create_dataset/dataframes/PRON/loro.tsv': {
        'loro': 'they; them',
    },
    'create_dataset/dataframes/PRON/lui.tsv': {
        'lui': 'he; him',
    },
    'create_dataset/dataframes/PRON/me.tsv': {
        'me': 'me',
    },
    'create_dataset/dataframes/PRON/mi.tsv': {
        'mi': 'me; to me',
        "m'": 'me; to me (before a vowel)',
    },
    'create_dataset/dataframes/PRON/ne.tsv': {
        'ne': 'of it; of them; some',
        "n'": 'of it; of them; some (before a vowel)',
    },
    'create_dataset/dataframes/PRON/nessuno.tsv': {
        'nessuno': 'no one; nobody (masculine)',
        'nessuna': 'no one; nobody (feminine)',
    },
    'create_dataset/dataframes/PRON/niente.tsv': {
        'niente': 'nothing; not anything',
        "nient'": 'nothing; not anything (before a vowel)',
    },
    'create_dataset/dataframes/PRON/noi.tsv': {
        'noi': 'we; us',
    },
    'create_dataset/dataframes/PRON/perché.tsv': {
        'perché': 'why',
    },
    'create_dataset/dataframes/PRON/poco.tsv': {
        'poco': 'a little; a bit (masculine)',
        'pochi': 'few (masculine plural)',
        'poche': 'few (feminine plural)',
        "po'": 'a little; a bit',
    },
    'create_dataset/dataframes/PRON/qual.tsv': {
        'qual': 'which',
    },
    'create_dataset/dataframes/PRON/qualcosa.tsv': {
        'qualcosa': 'something; anything',
    },
    'create_dataset/dataframes/PRON/quello.tsv': {
        'quello': 'that one (masculine)',
        'quel': 'that one (masculine, short form)',
        'quella': 'that one (feminine)',
        'quelle': 'those (feminine)',
        'quelli': 'those (masculine)',
    },
    'create_dataset/dataframes/PRON/questo.tsv': {
        'questo': 'this one (masculine)',
        'questa': 'this one (feminine)',
        'questi': 'these (masculine)',
        'queste': 'these (feminine)',
        'stella': 'star',
    },
    'create_dataset/dataframes/PRON/si.tsv': {
        'si': 'himself; herself; itself; themselves',
        "s'": 'himself; herself; itself; themselves (before a vowel)',
    },
    'create_dataset/dataframes/PRON/te.tsv': {
        'te': 'you',
    },
    'create_dataset/dataframes/PRON/ti.tsv': {
        'ti': 'you; to you',
        "t'": 'you; to you (before a vowel)',
    },
    'create_dataset/dataframes/PRON/tu.tsv': {
        'tu': 'you',
    },
    'create_dataset/dataframes/PRON/tutto.tsv': {
        'tutto': 'everything; all of it (masculine)',
        'tutti': 'everyone; all (masculine plural)',
        'tutte': 'everyone; all (feminine plural)',
        'tutta': 'all of it; everything (feminine)',
    },
    'create_dataset/dataframes/PRON/vi.tsv': {
        'vi': 'you; to you (plural)',
    },
    'create_dataset/dataframes/PRON/voi.tsv': {
        'voi': 'you (plural)',
    },
    'create_dataset/dataframes/SCONJ/che.tsv': {
        'che': 'that',
    },
    'create_dataset/dataframes/SCONJ/come.tsv': {
        'come': 'as; how',
        "com'": 'as; how (before a vowel)',
    },
    'create_dataset/dataframes/SCONJ/perché.tsv': {
        'perché': 'because; since',
    },
    'create_dataset/dataframes/SCONJ/quando.tsv': {
        'quando': 'when',
        "quand'": 'when (before a vowel)',
    },
    'create_dataset/dataframes/SCONJ/se.tsv': {
        'se': 'if; whether',
    },
}


def fill_translations(root='create_dataset/dataframes'):
    updated_files = []
    for path, mapping in TRANSLATIONS.items():
        if not os.path.exists(path):
            continue
        with open(path, encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            rows = list(reader)
        if not rows:
            continue
        header = rows[0]
        try:
            col_idx = header.index('translation_en')
        except ValueError:
            continue
        changed = False
        for i in range(1, len(rows)):
            row = rows[i]
            if len(row) <= col_idx:
                continue
            current = row[col_idx].strip()
            if current and 'singular' not in current.lower():
                continue
            text = row[0]
            if text not in mapping:
                if current:
                    continue
                raise KeyError(f"No translation provided for {text!r} in {path}")
            row[col_idx] = mapping[text]
            changed = True
        if changed:
            with open(path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows(rows)
            updated_files.append(path)
    return updated_files


def main():
    updated = fill_translations()
    if updated:
        print('Updated', len(updated), 'files')
    else:
        print('No updates needed')


if __name__ == '__main__':
    main()
