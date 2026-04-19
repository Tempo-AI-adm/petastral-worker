"""
PetAstral Worker — Render deployment
Processes pending jobs: geocoding → ephemeris → Gemini → save to Supabase.
"""

import json
import json as json_lib
import os
import re
import threading
import time
import urllib.request
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

import astro_calculator

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"

GEMINI_SYSTEM_INSTRUCTION = """Você é o motor de inteligência do SignoPet. Gere um guia de personalidade e comportamento para pets combinando Astrologia Ocidental com características genéticas da raça (cães) ou pelagem (gatos).

Tom: direto, caloroso, prático. Fale como um especialista em comportamento animal que também entende de astrologia — não como um astrólogo genérico. Use exemplos concretos do dia a dia do pet. Evite linguagem corporativa ou frases vagas.

Regras obrigatórias:
- Escreva sempre em português do Brasil
- Use o nome do pet ao longo do texto (não "seu pet" genérico)
- Cada capítulo mínimo 300 palavras com exemplos práticos reais
- Ao final de cada capítulo, escreva exatamente '### Dica Prática' em uma linha separada, seguido da dica em novo parágrafo. NUNCA repita as palavras 'Dica Prática' dentro do texto da dica em si.
- Evite previsões absolutas — use "tende a", "costuma", "pode demonstrar"
- Quando a raça for informada (não SRD), incorpore características comportamentais conhecidas dessa raça
- Quando for gato SRD, use a pelagem/cor como lente comportamental
- Quando o pet for um GATO, incorpore comportamentos específicos felinos: marcar território, amassar (fazer biscoito), ronronar como linguagem, piscar lento como demonstração de confiança, caça noturna e instinto predatório, vocalização específica (miar, trinar, rosnar), a relação com altura e territorialidade vertical, e a diferença fundamental entre gatos e cães na dinâmica com o tutor (gato escolhe quando interagir — não obedece, negocia)"""

SIGNOS_PT = {
    'Aries': 'Áries', 'Taurus': 'Touro', 'Gemini': 'Gêmeos',
    'Cancer': 'Câncer', 'Leo': 'Leão', 'Virgo': 'Virgem',
    'Libra': 'Libra', 'Scorpio': 'Escorpião', 'Sagittarius': 'Sagitário',
    'Capricorn': 'Capricórnio', 'Aquarius': 'Aquário', 'Pisces': 'Peixes'
}
ELEMENTOS_PT = {'Fire': 'Fogo', 'Earth': 'Terra', 'Air': 'Ar', 'Water': 'Água'}

# ---------------------------------------------------------------------------
# Supabase helpers (uses SERVICE ROLE key — bypasses RLS)
# ---------------------------------------------------------------------------

def _sb_headers(prefer="return=representation"):
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _sb_url(path):
    return os.environ["SUPABASE_URL"].rstrip("/") + path


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------

def claim_job(job_id):
    """Atomically set pending → processing. Returns True if claimed."""
    resp = requests.patch(
        _sb_url(f"/rest/v1/jobs?id=eq.{job_id}&status=eq.pending"),
        headers=_sb_headers(),
        json={"status": "processing"},
        timeout=10,
    )
    resp.raise_for_status()
    return len(resp.json()) > 0


def update_job(job_id, patch):
    resp = requests.patch(
        _sb_url(f"/rest/v1/jobs?id=eq.{job_id}"),
        headers=_sb_headers(),
        json=patch,
        timeout=10,
    )
    resp.raise_for_status()


def fail_job(job_id, message):
    update_job(job_id, {
        "status": "failed",
        "error_message": message[:500],
        "completed_at": _now_iso(),
    })


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def build_gemini_prompt(data, signs):
    hour_display = "não informado" if data.get("hour_unknown") else f"{data['hour']:02d}"
    minute_display = f"{data['minute']:02d}"

    pet_type = data['pet_type']
    breed = data['breed']
    is_srd = 'srd' in breed.lower() or 'vira' in breed.lower()
    signo_tutor = data.get('signo_tutor', '')
    cor = (data.get('pet_color') or '').lower()

    raca_contexto = ""
    if pet_type == 'dog' and not is_srd:
        grupos = {
            'energetico': ['Pinscher', 'Jack Russell', 'Chihuahua', 'Spitz'],
            'carente': ['Golden', 'Labrador', 'Lhasa', 'Beagle', 'Cocker'],
            'independente': ['Husky'],
            'dondoca': ['Shih Tzu', 'Poodle', 'Yorkshire', 'Maltês', 'Bichon'],
            'preguicoso': ['Bulldog', 'Basset', 'Pug', 'Dachshund', 'Salsicha'],
            'intenso': ['Border Collie', 'Pastor Alemão', 'Dálmata', 'Dobermann', 'Blue Heeler'],
            'caos': ['Rottweiler', 'Pitbull', 'Boxer', 'Corgi', 'Galgo', 'Sphynx', 'Bengal']
        }
        perfis = {
            "energetico": "Alta energia constante, reativo a qualquer estímulo sonoro ou visual. Late antes de pensar, guarda território mesmo sendo pequeno. Tende a se vincular intensamente a uma pessoa e ser possessivo com brinquedos. Não conhece o próprio tamanho — enfrenta cães maiores sem hesitar. Precisa de estímulo mental diário ou desenvolve ansiedade, lambedura excessiva e latido compulsivo. Pinscher: vigia nato, desconfia de tudo. Jack Russell: energia inesgotável, cava, escala. Chihuahua: grudento com o dono, agressivo com estranhos. Spitz: teatral, late para chamar atenção, manipulador emocional.",
            "carente": "Profundamente sociável, vive para aprovação humana. Ansiedade de separação é comum — pode uivar, roer e andar em círculos quando sozinho. Traz objetos como presente, cutuca para pedir carinho, segue o tutor de cômodo em cômodo. Extremamente motivado por comida. Golden/Labrador: carrega tudo na boca com delicadeza, 'boca mole'. Beagle: nariz comanda tudo, ignora comandos quando rastreia um cheiro. Cocker: sensibilidade emocional extrema, responde ao tom de voz. Lhasa: leal mas teimoso, guardião apesar do tamanho.",
            "independente": "Autônomo, tem agenda própria e não se submete por obediência. Vocal (uiva, 'fala', responde) mas não necessariamente obediente. Artista da fuga — pula cercas, abre portas, cava túneis. Mentalidade de matilha mas sem submissão. Instinto de caça alto, não confiável solto. Reações dramáticas a inconvenientes menores. Precisa de exercício intenso diário ou destrói a casa metodicamente.",
            "dondoca": "Busca conforto, prefere superfícies macias e calor. Sensível a mudanças no ambiente, barulho e manuseio brusco. Vínculo emocional forte com o cuidador principal — pode ter ciúmes. Poodle: extremamente inteligente, aprende rápido, manipula o dono com charme. Yorkshire: corajoso apesar do tamanho, territorial com a casa. Shih Tzu: criado para companhia, feliz no colo, não precisa de exercício intenso. Maltês: afetuoso, sensível, pode ser medroso. Bichon: alegre, palhaço, odeia ficar sozinho.",
            "preguicoso": "Baixa energia, prefere descanso. Testa limites com resistência passiva — deita no chão e se recusa a andar. Conforto é inegociável — melhor cama, melhor lugar. Bulldog: ronca, superaquece fácil, teimoso silencioso. Pug: palhaço natural, busca atenção apesar da preguiça, expressivo com os olhos. Dachshund: surpreendentemente teimoso e vocal pra seu tamanho, instinto de caça subterrânea. Basset: nariz comanda, pode ser mono-focado em um cheiro, ignora tudo ao redor.",
            "intenso": "Foco extremo, precisa de trabalho mental diário ou colapsa. Sem estímulo: pastoreia crianças, persegue sombras, destrói móveis por frustração. Aprende comandos em 1-3 repetições. Border Collie: olhar fixo intenso, tenta controlar movimento de tudo que se mexe. Pastor Alemão: protetor, territorial, se vincula a uma pessoa acima de todas. Dobermann: elegante e alerta, pode desenvolver ansiedade sem estrutura. Dálmata: energia explosiva, precisa de exercício intenso. Blue Heeler: morde calcanhares por instinto, precisa de trabalho ou inventa um.",
            "caos": "Energia imprevisível, testa hierarquia constantemente. Precisa de liderança firme e consistente ou preenche o vácuo. Rottweiler: exterior calmo, observador, protetor silencioso. Pitbull: extremamente carinhoso com a família, força de vontade enorme, pegajoso. Boxer: eterno filhote, pula, usa as patas como mãos, brincalhão até velho. Corgi: instinto de pastoreio, mandão, late para controlar situações. Galgo: calmo dentro de casa, explosivo no quintal, alma sensível por trás da velocidade.",
        }
        for grupo, racas in grupos.items():
            if any(r.lower() in breed.lower() for r in racas):
                raca_contexto = f"\nPERFIL COMPORTAMENTAL DA RAÇA ({breed}): {perfis[grupo]}\nUse esse perfil como contexto base ao longo de todos os capítulos — como a raça amplifica ou contrasta com os posicionamentos astrais."
                break
        if not raca_contexto:
            raca_contexto = f"\nRAÇA: {breed}. Incorpore características comportamentais conhecidas dessa raça ao longo dos capítulos."

    if pet_type == 'dog' and is_srd:
        raca_contexto = "\nPERFIL COMPORTAMENTAL (SRD / Vira-lata): Mistura genética cria coquetel comportamental único — impossível generalizar, mas padrões existem. Geralmente mais resiliente e adaptável que raças puras. Instintos de sobrevivência: pode guardar comida, marcar território com mais frequência, estar sempre alerta a ameaças. Inteligência social alta: lê emoções humanas com precisão. Tendência a ser independente mas leal quando conquista confiança. Cada SRD é genuinamente único — o mapa astral ganha importância extra porque a raça não define o comportamento.\nUse esse perfil como contexto base ao longo de todos os capítulos — como o instinto de sobrevivência e a adaptabilidade do SRD amplifica ou contrasta com os posicionamentos astrais."

    if pet_type == 'cat' and not is_srd:
        grupos_gato = {
            'comunicativo': ['Siamês', 'Siames'],
            'gigante_gentil': ['Maine Coon'],
            'relaxado': ['Ragdoll'],
            'selvagem': ['Bengal', 'Bengala'],
            'elegante': ['Persa', 'Angorá', 'Angora'],
            'sem_pelo': ['Sphynx'],
        }
        perfis_gato = {
            "comunicativo": "Extremamente vocal e interativo. Estabelece conversas longas com o tutor, exige atenção constante e sofre com solidão. Altamente inteligente e curioso. Não é adequado para tutores ausentes.",
            "gigante_gentil": "Sociável e dócil como um cão — segue o tutor pela casa, se dá bem com outros animais. Inteligente, aprende truques e adora água. Não é excessivamente carente mas gosta de estar perto.",
            "relaxado": "Temperamento extremamente calmo e tolerante. Literalmente relaxa no colo como boneco de pano. Não reage com agressividade, suporta manipulação. Muito sociável, boa opção para famílias.",
            "selvagem": "Altamente ativo e inteligente — precisa de 1-2h de estimulação diária ou se torna destrutivo. Adora água, aprende truques, age como cão. Territorial e pode marcar espaço. Não é gato de apartamento pequeno.",
            "elegante": "Calmo, quieto e reservado com estranhos. Prefere ambiente tranquilo e rotina previsível. Não demanda atenção mas aprecia carinho suave. Independente sem ser frio.",
            "sem_pelo": "Um dos gatos mais sociáveis e afetivos que existem. Segue o tutor em tudo, adora colo e calor humano. Extrovertido com estranhos. Precisa de ambiente quente e banhos regulares pela pele oleosa.",
        }
        for grupo, racas in grupos_gato.items():
            if any(r.lower() in breed.lower() for r in racas):
                raca_contexto = f"\nPERFIL COMPORTAMENTAL DA RAÇA ({breed}): {perfis_gato[grupo]}\nUse esse perfil como contexto base ao longo de todos os capítulos — como a raça amplifica ou contrasta com os posicionamentos astrais."
                break
        if not raca_contexto:
            raca_contexto = f"\nRAÇA: {breed}. Incorpore características comportamentais conhecidas dessa raça ao longo dos capítulos."

    pelagem_contexto = ""
    if pet_type == 'dog' and is_srd:
        pelagem_dog = {
            "caramelo": "O icônico vira-lata caramelo brasileiro — sociável, adaptável, o cachorro que se dá bem com todo mundo. Emocionalmente resiliente mas busca afeto constante.",
            "preto": "Tende a ser calmo, leal e observador. Guardião por natureza. Muitas vezes subestimado mas profundamente conectado ao dono.",
            "branco": "Pode ser mais sensível a estímulos ambientais. Tende a ser mais cauteloso e reservado inicialmente.",
            "cinza": "Temperamento equilibrado e adaptável. Observador antes de agir.",
            "marrom": "Ativo, curioso, explorador. Gosta de investigar tudo que é novo no ambiente.",
            "creme": "Dócil e tranquilo. Tende a ser mais suave nas reações e nas brincadeiras.",
        }
        cores_lista = [c.strip() for c in cor.split(',') if c.strip()]
        if len(cores_lista) > 1:
            descricoes = [pelagem_dog[c] for c in cores_lista if c in pelagem_dog]
            if descricoes:
                pelagem_contexto = f"\nPERFIL COMPORTAMENTAL POR PELAGEM ({cor}): combinação de {' / '.join(cores_lista)}. {' '.join(descricoes)}\nIncorpore essas características de forma orgânica ao longo do texto — mencione a combinação de cores apenas uma vez, de forma natural, sem repetir como tag a cada parágrafo."
        else:
            for key, desc in pelagem_dog.items():
                if key in cor:
                    pelagem_contexto = f"\nPERFIL COMPORTAMENTAL POR PELAGEM ({cor}): {desc}\nIncorpore essa característica de forma orgânica ao longo do texto — mencione a pelagem apenas uma vez no laudo inteiro, de forma natural, sem repetir como tag a cada parágrafo."
                    break
    if pet_type == 'cat' and is_srd:
        pelagem_gato = {
            "preto": "Independente e observador. Escolhe quando interagir — não responde bem a atenção forçada. Na tradição popular é considerado místico e protetor. Tende a ser mais cauteloso com estranhos, mas profundamente leal quando se vincula.",
            "branco": "Sensível a barulho e mudanças de ambiente. Precisa de previsibilidade e silêncio. Pode ser mais reservado. Elegante e seletivo com quem interage.",
            "cinza": "Temperamento equilibrado — nem grudento nem distante. Adaptável a diferentes ambientes. Temperamento similar ao Russian Blue mesmo em SRD. Observador e tranquilo.",
            "caramelo": "O 'golden retriever dos gatos' — social, expressivo, pede atenção ativamente. 80% dos gatos laranjas são machos. Conhecido por ser atrapalhado, brincalhão e goofar. Personalidade grande.",
            "marrom": "Curioso e explorador. Investiga território e objetos novos. Ativo e inquieto. Precisa de enriquecimento ambiental.",
            "creme": "Dócil e sereno. Tende a ser mais calmo e menos reativo que outras pelagens. Aprecia rotina e tranquilidade.",
            "tigrado": "Instinto de caça pronunciado. Territorial, precisa de espaço e estímulo para caçar. Tigrados laranjas: extrovertidos e bagunceiros, personalidade de palhaço. Tigrados marrons/cinza: mais equilibrados e caçadores metódicos.",
        }
        cores_lista = [c.strip() for c in cor.split(',') if c.strip()]
        if len(cores_lista) > 1:
            descricoes = [pelagem_gato[c] for c in cores_lista if c in pelagem_gato]
            if descricoes:
                pelagem_contexto = f"\nPERFIL COMPORTAMENTAL POR PELAGEM ({cor}): combinação de {' / '.join(cores_lista)}. {' '.join(descricoes)}\nIncorpore essas características de forma orgânica ao longo do texto — mencione a combinação de cores apenas uma vez, de forma natural, sem repetir como tag a cada parágrafo."
        else:
            for key, desc in pelagem_gato.items():
                if key in cor:
                    pelagem_contexto = f"\nPERFIL COMPORTAMENTAL POR PELAGEM ({cor}): {desc}\nIncorpore essa característica de forma orgânica ao longo do texto — mencione a pelagem apenas uma vez no laudo inteiro, de forma natural, sem repetir como tag a cada parágrafo."
                    break

    pet_name = data['pet_name'].strip().title()

    return f"""DADOS DO PET:
Nome: {pet_name}
Tipo: {pet_type}
Raça/Pelagem: {breed}
Sexo: {data['sex']}
Cor: {data.get('pet_color') or 'não informado'}
Marcações: {data.get('pet_markings') or 'não informado'}
Data de Nascimento: {data['day']:02d}/{data['month']:02d}/{data['year']} às {hour_display}:{minute_display}h
Local: {data['city']}, {data['country']}
{raca_contexto}{pelagem_contexto}

DADOS ASTRAIS CALCULADOS:
- Sol em {signs['sun']}
- Lua em {signs['moon']}
- Mercúrio em {signs['mercury']}
- Vênus em {signs['venus']}
- Marte em {signs['mars']}
- Júpiter em {signs['jupiter']}
- Saturno em {signs['saturn']}
- Urano em {signs['uranus']}
- Netuno em {signs['neptune']}
- Plutão em {signs['pluto']}
- Elemento Predominante: {signs['dominant_element']}

TAREFA: GERE O GUIA SIGNOPET COMPLETO

REGRAS INEGOCIÁVEIS:
- NUNCA gere texto genérico que serviria para qualquer pet. Cada parágrafo deve ter pelo menos um cruzamento específico entre signo e raça/pelagem.
- Use o nome {pet_name} em todo o texto — nunca "o pet" ou "o animal".
- Quando mencionar um traço comportamental da raça ou pelagem, contextualize brevemente de onde vem essa informação (ex: "criadores de Border Collie relatam que...", "a etologia felina associa gatos tigrados a...", "estudos sobre temperamento canino mostram que..."). Faça isso de forma orgânica, 1-2 vezes por capítulo, sem parecer artigo científico.
- O tom é de um astrólogo que entende de comportamento animal — fluido, acessível, com autoridade mas sem jargão técnico pesado.
- Cada capítulo deve ter exemplos concretos do dia a dia que o tutor vai reconhecer imediatamente ("você pode notar que...", "não estranhe se...").
- NUNCA repita a mesma informação em capítulos diferentes. Se mencionou um traço no capítulo 1, não repita nos seguintes.
- O tutor é de {signo_tutor} — use essa informação de forma sutil ao longo do texto, especialmente nos capítulos 2 (vínculo) e 5 (relacionamentos).

COMO FAZER CRUZAMENTOS:
Para cada posicionamento astral, pergunte-se: "como esse traço se manifesta ESPECIFICAMENTE em um {breed} {cor}?" A resposta nunca deve ser genérica.
- Exemplo RUIM: "Com Sol em Áries, {pet_name} é cheio de energia e iniciativa."
- Exemplo BOM: "Com Sol em Áries, {pet_name} canaliza a teimosia natural do Dachshund em uma determinação que beira o cômico — ele pode passar 20 minutos tentando alcançar algo debaixo do sofá, usando seu corpo longo como ferramenta de escavação, recusando qualquer ajuda."

FORMATO DE SAÍDA OBRIGATÓRIO — siga exatamente esta estrutura, preenchendo o conteúdo entre os marcadores:

##VISAO_ASTRAL_START##
PERSONALIDADE: [frase direta cruzando signo solar + raça + uma característica comportamental específica de {pet_name}]
EMOCOES: [frase cruzando Lua + como a raça/pelagem influencia a expressão emocional]
ENERGIA: [frase cruzando Marte + nível energético real da raça]
RELACIONAMENTO: [frase cruzando Vênus + como a raça se vincula ao tutor]
##VISAO_ASTRAL_END##

ATENÇÃO CRÍTICA: cada capítulo DEVE começar com ##CAPITULO_START## e DEVE terminar com ##CAPITULO_END## — sem exceção. Se faltar qualquer um desses marcadores, o laudo inteiro será invalidado. Gere TODOS os 9 capítulos completos.

Para cada capítulo abaixo, use exatamente este bloco:

##CAPITULO_START##
NUMERO: [número]
TITULO: [título]
CONTEUDO:
[mínimo 250 palavras em português brasileiro. Cada parágrafo deve conter pelo menos um cruzamento signo × raça/pelagem. Exemplos concretos do dia a dia. Termine com ### Dica Prática seguida de uma dica específica para essa combinação de raça+signo — nunca uma dica genérica que serviria para qualquer animal.]
##CAPITULO_END##

Capítulos a gerar (TODOS os 9 obrigatórios):
1. Sol em {signs['sun']}: Essência, Comportamento e Personalidade
   → Foco: como o signo solar molda o temperamento base da raça. O que amplifica, o que contrasta.
2. Lua em {signs['moon']}: Emoções, Necessidades e Vínculo com o Tutor
   → Foco: mundo emocional interno, como busca conforto, relação com o tutor de {signo_tutor}.
3. Elementos Astrológicos: O Ambiente e a Energia Ideal
   → Foco: ambiente físico ideal para essa raça+elemento. Concreto: tipo de cama, lugar preferido, rotina ideal.
4. Mercúrio em {signs['mercury']}: Como {pet_name} Se Comunica
   → Foco: vocalizações, linguagem corporal, como a raça expressa necessidades. Específico por raça.
5. Vênus em {signs['venus']}: Relacionamentos e Conexões
   → Foco: como demonstra afeto, ciúmes, relação com outros pets e pessoas. Dinâmica com tutor de {signo_tutor}.
6. Marte em {signs['mars']}: Energia, Atividade e Comportamento
   → Foco: nível de exercício real da raça, brincadeiras preferidas, assertividade.
7. Júpiter em {signs['jupiter']}: Sorte, Descobertas e Expansão
   → Foco: o que faz esse pet florescer, onde encontra alegria, momentos de expansão.
8. Saturno em {signs['saturn']}: Desafios e Aprendizados
   → Foco: medos reais da raça, traumas comuns, desafios comportamentais específicos.
9. Urano, Netuno e Plutão: Transformações e Propósito
   → Foco: mudanças ao longo da vida, intuição, propósito mais profundo. Capítulo de encerramento com tom emocional."""


def _parse_gemini_response(raw_text):
    """Parse custom-delimited Gemini response into JSON v1. Falls back to raw text."""
    raw = raw_text.strip()

    try:
        result = {'schema_version': 'v1', 'visao_astral': {}, 'capitulos': []}

        # Extract visao_astral block
        va_match = re.search(r'##VISAO_ASTRAL_START##(.*?)##VISAO_ASTRAL_END##', raw, re.DOTALL)
        if va_match:
            va_text = va_match.group(1).strip()
            for field in ['PERSONALIDADE', 'EMOCOES', 'ENERGIA', 'RELACIONAMENTO']:
                field_match = re.search(rf'{field}:\s*(.+?)(?=\s(?:EMOCOES|ENERGIA|RELACIONAMENTO|PERSONALIDADE):|##VISAO_ASTRAL_END##|$)', va_text, re.DOTALL)
                if field_match:
                    result['visao_astral'][field.lower()] = field_match.group(1).strip()

        # Extract capitulo blocks
        cap_matches = re.findall(r'##CAPITULO_START##(.*?)##CAPITULO_END##', raw, re.DOTALL)
        if len(cap_matches) < 8:
            fallback = [p.split('##CAPITULO_END##')[0] for p in raw.split('##CAPITULO_START##')[1:]]
            print(f"[parse] regex encontrou {len(cap_matches)}, fallback split encontrou {len(fallback)}", flush=True)
            if len(fallback) > len(cap_matches):
                cap_matches = fallback
        for cap in cap_matches:
            numero_match   = re.search(r'NUMERO:\s*(\d+)', cap)
            titulo_match   = re.search(r'TITULO:\s*(.+?)(?=\nCONTEUDO:|CONTEUDO:)', cap, re.DOTALL)
            conteudo_match = re.search(r'CONTEUDO:\s*(.*)', cap, re.DOTALL)
            if numero_match and titulo_match and conteudo_match:
                conteudo = conteudo_match.group(1).strip()
                conteudo = re.sub(
                    r'(###\s*Dica Prática\s*\n+)\s*(?:Dica Prática\s*|#+\s*Dica Prática\s*)',
                    r'\1',
                    conteudo,
                    flags=re.IGNORECASE
                )
                conteudo = re.sub(
                    r'(###\s*Dica Prática\s*\n+)Dica Prática[\s:]+',
                    r'\1',
                    conteudo,
                    flags=re.IGNORECASE
                )
                result['capitulos'].append({
                    'numero': int(numero_match.group(1)),
                    'titulo': titulo_match.group(1).strip(),
                    'conteudo': conteudo,
                })

        if not cap_matches:
            print(f"[parse] raw snippet: {raw[:500]}", flush=True)
        print(f'[parse] capítulos encontrados: {len(result["capitulos"])}', flush=True)
        if result['capitulos']:
            return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        print(f'[parse] erro no parser customizado: {exc}', flush=True)

    # Fallback: return raw text
    return raw_text


def _call_gemini_model(prompt, model, base_url, api_key):
    """Try one model with 3 attempts (10s/20s/40s backoff). Returns text or raises."""
    url = f"{base_url}{model}:generateContent"
    delays = [10, 20, 40]
    for attempt, delay in enumerate(delays, start=1):
        print(f"[Gemini] model={model} attempt {attempt}/3 -> {url}", flush=True)
        try:
            resp = requests.post(
                url,
                params={"key": api_key},
                headers={
                    "User-Agent": "PetAstral-Worker/1.0",
                    "Accept": "application/json",
                },
                json={
                    "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_INSTRUCTION}]},
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.7, "maxOutputTokens": 16000},
                },
                timeout=120,
            )
            if resp.status_code == 503 and attempt < len(delays):
                print(f"[Gemini] 503 on attempt {attempt} - body: {resp.text[:500]}", flush=True)
                raise requests.exceptions.HTTPError(
                    f"503 Service Unavailable (attempt {attempt})", response=resp
                )
            resp.raise_for_status()
            result = resp.json()
            try:
                text = result["candidates"][0]["content"]["parts"][0]["text"]
                usage = result.get("usageMetadata", {})
                print(f"[gemini] tokens — prompt: {usage.get('promptTokenCount', '?')}, output: {usage.get('candidatesTokenCount', '?')}, total: {usage.get('totalTokenCount', '?')}", flush=True)
                return text
            except (KeyError, IndexError) as exc:
                raise RuntimeError(f"Unexpected Gemini response: {result}") from exc
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            print(f"[Gemini] error on attempt {attempt}: {exc}", flush=True)
            if attempt < len(delays):
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"model={model} failed after {len(delays)} attempts: {exc}"
                ) from exc


def call_gemini(prompt):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    primary_model  = "gemini-2.5-flash"
    fallback_model = "gemini-2.5-flash-lite"
    fallback_url   = GEMINI_BASE_URL

    try:
        result = _call_gemini_model(prompt, primary_model, GEMINI_BASE_URL, api_key)
        print(f"[Gemini] success with primary model: {primary_model}", flush=True)
        return result, primary_model
    except RuntimeError as primary_exc:
        print(f"[Gemini] primary model failed: {primary_exc}. Trying fallback {fallback_model}", flush=True)

    result = _call_gemini_model(prompt, fallback_model, fallback_url, api_key)
    print(f"[Gemini] success with fallback model: {fallback_model}", flush=True)
    return result, fallback_model


# ---------------------------------------------------------------------------
# Save to Supabase (owners → pets → reports)
# ---------------------------------------------------------------------------

def save_to_supabase(data, signs, report_text, model_used=None):
    headers = _sb_headers()

    # 1. Upsert owner (on conflict email, just return existing)
    utm_source   = data.get('utmSource', '')
    utm_medium   = data.get('utmMedium', '')
    utm_campaign = data.get('utmCampaign', '')
    referrer     = data.get('referrer', '')
    try:
        owner_resp = requests.post(
            _sb_url("/rest/v1/owners"),
            headers={**headers, "Prefer": "resolution=merge-duplicates,return=representation"},
            json={
                "name":         data["owner_name"],
                "email":        data["owner_email"],
                "utm_source":   utm_source,
                "utm_medium":   utm_medium,
                "utm_campaign": utm_campaign,
                "referrer":     referrer,
            },
            timeout=15,
        )
        if owner_resp.status_code in (200, 201) and owner_resp.json():
            owner_id = owner_resp.json()[0]["id"]
        else:
            raise ValueError("empty response")
    except Exception:
        # fallback: busca owner existente pelo email
        fallback = requests.get(
            _sb_url("/rest/v1/owners"),
            headers=headers,
            params={"email": f"eq.{data['owner_email']}", "select": "id"},
            timeout=15,
        )
        fallback.raise_for_status()
        rows = fallback.json()
        if not rows:
            raise RuntimeError(f"Owner not found and insert failed for {data['owner_email']}")
        owner_id = rows[0]["id"]

    # 2. Insert pet (includes owner_email for RLS read access later)
    birth_data = {
        k: data[k] for k in ("city", "country", "year", "month", "day", "hour", "minute")
    }
    birth_data["hour_unknown"] = data.get("hour_unknown", False)

    pet_payload = {
        "owner_id":      owner_id,
        "owner_email":   data["owner_email"],
        "name":          data["pet_name"],
        "type":          data["pet_type"],
        "breed":         data["breed"],
        "sex":           data["sex"],
        "pet_color":     data.get("pet_color"),
        "pet_markings":  data.get("pet_markings"),
        "birth_data":    birth_data,
    }
    print(f"[save_to_supabase] pet_payload={pet_payload}", flush=True)
    pet_resp = requests.post(
        _sb_url("/rest/v1/pets"),
        headers=headers,
        json=pet_payload,
        timeout=15,
    )
    if not pet_resp.ok:
        raise Exception(f"{pet_resp.status_code} {pet_resp.reason} — {pet_resp.text}")
    pet_id = pet_resp.json()[0]["id"]

    # 3. Insert report
    report_resp = requests.post(
        _sb_url("/rest/v1/reports"),
        headers=headers,
        json={
            "pet_id":      pet_id,
            "signs":       signs,
            "report_text": report_text,
            "model_used":  model_used or "unknown",
            "created_at":  _now_iso(),
        },
        timeout=15,
    )
    report_resp.raise_for_status()
    report_id = report_resp.json()[0]["id"]

    return report_id, pet_id


# ---------------------------------------------------------------------------
# pet_data mapper (sessionStorage → internal data dict)
# ---------------------------------------------------------------------------

def _map_pet_data(pet_data, email):
    """Convert sessionStorage pet_data format to the internal data dict.

    Required internal fields consumed by build_gemini_prompt / save_to_supabase:
      pet_name, pet_type, breed, sex, pet_color, pet_markings,
      city, country, year, month, day, hour, minute, hour_unknown,
      owner_name, owner_email
    """
    current_year = datetime.now(timezone.utc).year

    # "ano" is collected in the form's step 2 (month + year).
    # Fall back to the current year if the field is absent.
    year = pet_data.get("ano")
    year = int(year) if year else current_year

    return {
        "pet_name":     pet_data.get("nome") or "",
        "pet_type":     pet_data.get("tipo") or "",
        "breed":        pet_data.get("raca") or "",
        "sex":          "female" if pet_data.get("sexo") == "femea" else "male",
        "pet_color":    ", ".join(pet_data["cor"]) if isinstance(pet_data.get("cor"), list) else (pet_data.get("cor") or ""),
        "pet_markings": pet_data.get("pelo"),
        "city":         pet_data.get("cidade") or "",
        "country":      "Brazil",
        "year":         year,
        "month":        int(pet_data.get("mes") or 1),
        "day":          int(pet_data.get("dia") or 1),
        "hour":         12,
        "minute":       0,
        "hour_unknown": True,
        "owner_name":   "",
        "owner_email":  email,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "petastral-worker"})


@app.route("/process", methods=["POST", "OPTIONS"])
def process_job():
    # CORS preflight
    if request.method == "OPTIONS":
        resp = jsonify({})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 204

    body = request.get_json(silent=True) or {}
    job_id = body.get("job_id")
    if not job_id:
        return jsonify({"error": "Missing job_id"}), 400

    # 1. Claim job (pending → processing)
    try:
        if not claim_job(job_id):
            return jsonify({"error": "Job not found or already processing"}), 409
    except Exception as exc:
        return jsonify({"error": f"Claim failed: {exc}"}), 502

    # 2. Fetch input_data from jobs table
    try:
        resp = requests.get(
            _sb_url(f"/rest/v1/jobs?id=eq.{job_id}&select=input_data"),
            headers=_sb_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return jsonify({"error": "Job not found"}), 404
        data = rows[0]["input_data"]
    except Exception as exc:
        fail_job(job_id, str(exc))
        return jsonify({"error": f"Fetch failed: {exc}"}), 502

    # 3. Astro calculation (geocoding → ephemeris → signs)
    try:
        raw_signs = astro_calculator.calculate(
            city=data["city"],
            country=data["country"],
            year=data["year"],
            month=data["month"],
            day=data["day"],
            hour=data.get("hour", 12),
            minute=data.get("minute", 0),
        )
    except Exception as exc:
        fail_job(job_id, f"Astro calc failed: {exc}")
        return jsonify({"error": f"Astro calculation failed: {exc}"}), 422

    signs = {
        "sun":              raw_signs["sun_sign"],
        "moon":             raw_signs["moon_sign"],
        "mercury":          raw_signs["mercury_sign"],
        "venus":            raw_signs["venus_sign"],
        "mars":             raw_signs["mars_sign"],
        "jupiter":          raw_signs["jupiter_sign"],
        "saturn":           raw_signs["saturn_sign"],
        "uranus":           raw_signs["uranus_sign"],
        "neptune":          raw_signs["neptune_sign"],
        "pluto":            raw_signs["pluto_sign"],
        "dominant_element": raw_signs["dominant_element"],
    }
    signs = {k: SIGNOS_PT.get(v, v) for k, v in signs.items()}
    signs["dominant_element"] = ELEMENTOS_PT.get(signs["dominant_element"], signs["dominant_element"])

    # 4. Gemini report generation
    try:
        report_text_raw, model_used = call_gemini(build_gemini_prompt(data, signs))
        report_text = _parse_gemini_response(report_text_raw)
    except Exception as exc:
        fail_job(job_id, f"Gemini failed: {exc}")
        return jsonify({"error": f"Gemini error: {exc}"}), 502

    # 5. Save to owners/pets/reports
    try:
        report_id, pet_id = save_to_supabase(data, signs, report_text, model_used=model_used)
    except Exception as exc:
        fail_job(job_id, f"Save failed: {exc}")
        return jsonify({"error": f"Supabase save error: {exc}"}), 502

    # 6. Mark job complete
    output = {
        "report_id": report_id,
        "pet_id":    pet_id,
        "signs":     signs,
    }
    update_job(job_id, {
        "status":      "completed",
        "output_data": output,
        "completed_at": _now_iso(),
    })

    return jsonify({"job_id": job_id, "status": "completed", **output})


def _send_email(base_url, email, pet_nome, report_id):
    try:
        payload = json_lib.dumps({
            'email': email,
            'pet_nome': pet_nome,
            'report_id': report_id
        }).encode('utf-8')
        req = urllib.request.Request(
            f'{base_url}/api/payment/email',
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        urllib.request.urlopen(req, timeout=10)
        print(f'[email] enviado para {email}')
    except Exception as e:
        print(f'[email] erro ao enviar: {e}')


def _process_generate(payment_id, pet_data, email):
    try:
        # 1. Map sessionStorage → internal data dict
        data = _map_pet_data(pet_data, email)

        # 2. Astro calculation (geocoding → ephemeris → signs)
        raw_signs = astro_calculator.calculate(
            city=data["city"],
            country=data["country"],
            year=data["year"],
            month=data["month"],
            day=data["day"],
            hour=data["hour"],
            minute=data["minute"],
        )
        signs = {
            "sun":              raw_signs["sun_sign"],
            "moon":             raw_signs["moon_sign"],
            "mercury":          raw_signs["mercury_sign"],
            "venus":            raw_signs["venus_sign"],
            "mars":             raw_signs["mars_sign"],
            "jupiter":          raw_signs["jupiter_sign"],
            "saturn":           raw_signs["saturn_sign"],
            "uranus":           raw_signs["uranus_sign"],
            "neptune":          raw_signs["neptune_sign"],
            "pluto":            raw_signs["pluto_sign"],
            "dominant_element": raw_signs["dominant_element"],
        }
        signs = {k: SIGNOS_PT.get(v, v) for k, v in signs.items()}
        signs["dominant_element"] = ELEMENTOS_PT.get(signs["dominant_element"], signs["dominant_element"])

        # 3. Gemini report generation — até 3 tentativas por qualidade
        prompt = build_gemini_prompt(data, signs)
        report_text = None
        model_used = None
        _capitulos_count = 0
        for gen_attempt in range(3):
            report_text_raw, model_used = call_gemini(prompt)
            report_text = _parse_gemini_response(report_text_raw)
            try:
                _parsed = json.loads(report_text)
                _capitulos_count = len(_parsed.get("capitulos", []))
            except Exception:
                _capitulos_count = 0

            print(f"[generate] tentativa {gen_attempt+1}/3 — capítulos: {_capitulos_count}", flush=True)

            if _capitulos_count >= 8:
                break

            if gen_attempt < 2:
                print(f"[generate] capítulos insuficientes, retentando...", flush=True)
                time.sleep(5)

        # 4. Validate parse before saving
        if _capitulos_count < 8:
            print(f"[generate] FALHA: {_capitulos_count} capítulos após 3 tentativas", flush=True)
            requests.patch(
                _sb_url(f"/rest/v1/payments?id=eq.{payment_id}"),
                headers={**_sb_headers(), "Prefer": "return=minimal"},
                json={"laudo_status": "failed", "status": "failed"},
                timeout=10,
            )
            try:
                requests.post(
                    os.environ.get('FRONTEND_URL', 'https://petastral-signos.vercel.app') + '/api/payment/email',
                    json={"to": "signopet@gmail.com", "subject": f"FALHA LAUDO - {pet_data.get('pet_name', 'pet')} ({email})", "html": f"<p>Payment {payment_id} falhou após 3 tentativas. {_capitulos_count} capítulos gerados.</p><p>Email cliente: {email}</p>"},
                    timeout=10,
                )
            except Exception:
                pass
            return

        # 5. Save owners → pets → reports
        report_id, _pet_id = save_to_supabase(data, signs, report_text, model_used=model_used)

        # 6. Link report back to the payment row
        for attempt in range(3):
            try:
                patch_resp = requests.patch(
                    _sb_url(f"/rest/v1/payments?id=eq.{payment_id}"),
                    headers=_sb_headers(),
                    json={"report_id": report_id, "laudo_status": "success"},
                    timeout=15,
                )
                patch_resp.raise_for_status()
                print(f"[generate] payment {payment_id} patched to success", flush=True)
                break
            except Exception as exc:
                print(f"[generate] WARNING: payment patch attempt {attempt+1} failed for {payment_id}: {exc}", flush=True)
                if hasattr(exc, 'response') and exc.response is not None:
                    print(f"[generate] Response status: {exc.response.status_code}, body: {exc.response.text[:500]}", flush=True)
                if attempt < 2:
                    import time
                    time.sleep(2)

        base_url = os.environ.get('FRONTEND_URL', 'https://petastral-signos.vercel.app')
        _send_email(base_url, email, data.get('pet_name', ''), report_id)

        print(f"[generate] done — payment_id={payment_id} report_id={report_id}", flush=True)

    except Exception as exc:
        print(f"[generate] ERROR background processing for payment_id={payment_id}: {exc}", flush=True)
        try:
            requests.patch(
                _sb_url(f"/rest/v1/payments?id=eq.{payment_id}"),
                headers={**_sb_headers(), "Prefer": "return=minimal"},
                json={"laudo_status": "failed", "status": "failed"},
                timeout=10,
            )
        except Exception:
            pass


@app.route("/generate", methods=["POST", "OPTIONS"])
def generate():
    # CORS preflight
    if request.method == "OPTIONS":
        resp = jsonify({})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp, 204

    body = request.get_json(silent=True) or {}
    payment_id = body.get("payment_id")
    pet_data   = body.get("pet_data") or {}
    email      = body.get("email") or pet_data.get("email")

    if not payment_id:
        return jsonify({"error": "Missing payment_id"}), 400
    if not pet_data:
        return jsonify({"error": "Missing pet_data"}), 400
    if not email:
        return jsonify({"error": "Missing email"}), 400

    threading.Thread(
        target=_process_generate,
        args=(payment_id, pet_data, email),
        daemon=True,
    ).start()

    return jsonify({"ok": True, "message": "processing"}), 202


# ---------------------------------------------------------------------------
# Local dev
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
