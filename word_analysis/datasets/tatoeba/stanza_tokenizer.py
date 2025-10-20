# !pip install stanza
import stanza
import pandas as pd
import uuid


stanza.download("it")  # once


def get_token_hash(term, lemma, pos):

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

def get_group_hash(lemma, pos):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}-{pos}"))


class StanzaTokenizer:
    def __init__(self):
        self.nlp = stanza.Pipeline("it", processors="tokenize,pos,lemma,depparse")

    def tokenize(self, text):
        doc = self.nlp(text)
        tokens = []
        for sentence in doc.sentences:
            for token in sentence.tokens:
                for word in token.words:
                    text = word.text.lower()
                    lemma = word.lemma.lower()
                    tokens.append({
                        "text": text,
                        "lemma": lemma,
                        "pos": word.upos,
                        "xpos": word.xpos,
                        "head": word.head,
                        "deprel": word.deprel,
                        "feats": word.feats,
                        "vector": None,  # Stanza does not provide word vectors
                        "token_hash": get_token_hash(text, lemma, word.upos),
                        "group_hash": get_group_hash(lemma, word.upos),
                    })
        return tokens
    
    def tokenize_to_df(self, text):
        tokens = self.tokenize(text)
        data = {
            key: [] for key in tokens[0].keys()
        }
        for token in tokens:
            for key, value in token.items():
                data[key].append(value)
        df = pd.DataFrame(data)
        return df