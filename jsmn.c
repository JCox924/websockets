//
// Created by Joshua Cox on 12/15/25.
//
#include "jsmn.h"

static jsmntok_t *jsmn_alloc_token(jsmn_parser *parser, jsmntok_t *tokens,
                                  unsigned int num_tokens) {
    if (parser->toknext >= num_tokens) return 0;
    jsmntok_t *tok = &tokens[parser->toknext++];
    tok->start = tok->end = -1;
    tok->size = 0;
    tok->type = JSMN_UNDEFINED;
    return tok;
}

static void jsmn_fill_token(jsmntok_t *token, jsmntype_t type, int start, int end) {
    token->type = type;
    token->start = start;
    token->end = end;
    token->size = 0;
}

void jsmn_init(jsmn_parser *parser) {
    parser->pos = 0;
    parser->toknext = 0;
    parser->toksuper = -1;
}

static int jsmn_parse_primitive(jsmn_parser *parser, const char *js, int len,
                               jsmntok_t *tokens, unsigned int num_tokens) {
    int start = (int)parser->pos;
    for (; parser->pos < (unsigned int)len; parser->pos++) {
        switch (js[parser->pos]) {
            case '\t': case '\r': case '\n': case ' ':
            case ',': case ']': case '}':
                goto found;
            default:
                break;
        }
    }
found:
    if (tokens) {
        jsmntok_t *tok = jsmn_alloc_token(parser, tokens, num_tokens);
        if (!tok) return -1;
        jsmn_fill_token(tok, JSMN_PRIMITIVE, start, (int)parser->pos);
        if (parser->toksuper != -1) tokens[parser->toksuper].size++;
    }
    parser->pos--;
    return 0;
}

static int jsmn_parse_string(jsmn_parser *parser, const char *js, int len,
                            jsmntok_t *tokens, unsigned int num_tokens) {
    int start = (int)parser->pos;
    parser->pos++;

    for (; parser->pos < (unsigned int)len; parser->pos++) {
        char c = js[parser->pos];

        if (c == '\"') {
            if (tokens) {
                jsmntok_t *tok = jsmn_alloc_token(parser, tokens, num_tokens);
                if (!tok) return -1;
                jsmn_fill_token(tok, JSMN_STRING, start + 1, (int)parser->pos);
                if (parser->toksuper != -1) tokens[parser->toksuper].size++;
            }
            return 0;
        }

        if (c == '\\' && parser->pos + 1 < (unsigned int)len) {
            parser->pos++; /* skip escaped char */
        }
    }
    return -1;
}

int jsmn_parse(jsmn_parser *parser, const char *js, int len,
               jsmntok_t *tokens, unsigned int num_tokens) {
    for (; parser->pos < (unsigned int)len; parser->pos++) {
        char c = js[parser->pos];
        jsmntok_t *tok;

        switch (c) {
            case '{': case '[':
                tok = jsmn_alloc_token(parser, tokens, num_tokens);
                if (!tok) return -1;
                tok->type = (c == '{') ? JSMN_OBJECT : JSMN_ARRAY;
                tok->start = (int)parser->pos;
                tok->end = -1;
                tok->size = 0;
                if (parser->toksuper != -1) tokens[parser->toksuper].size++;
                parser->toksuper = (int)(parser->toknext - 1);
                break;

            case '}': case ']': {
                jsmntype_t type = (c == '}') ? JSMN_OBJECT : JSMN_ARRAY;
                int i;
                for (i = (int)parser->toknext - 1; i >= 0; i--) {
                    tok = &tokens[i];
                    if (tok->start != -1 && tok->end == -1) {
                        if (tok->type != type) return -1;
                        tok->end = (int)parser->pos + 1;
                        parser->toksuper = -1;
                        for (int j = i - 1; j >= 0; j--) {
                            if (tokens[j].start != -1 && tokens[j].end == -1) {
                                parser->toksuper = j;
                                break;
                            }
                        }
                        break;
                    }
                }
                if (i == -1) return -1;
                break;
            }

            case '\"':
                if (jsmn_parse_string(parser, js, len, tokens, num_tokens) < 0) return -1;
                break;

            case '\t': case '\r': case '\n': case ' ': case ':': case ',':
                break;

            default:
                if (jsmn_parse_primitive(parser, js, len, tokens, num_tokens) < 0) return -1;
                break;
        }
    }
    return (int)parser->toknext;
}
