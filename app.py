import streamlit as st
import pandas as pd
from datetime import datetime
import json
import os

CONFIG_FILE = 'modules_config.json'

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}

    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=4)

def calculate_duration(time_range_srt):
    try:
        if not isinstance(time_range_srt, str):
            return 0
        start_str, end_str = time_range_srt.split('-')
        fmt = '%H:%M'
        t_start = datetime.strptime(start_str.strip(), fmt)
        t_end = datetime.strptime(end_str.strip(), fmt)
        return (t_end - t_start).total_seconds() / 3600
    except Exception:
        return 0

def highlight_rows(row):
    color = 'background-color: transparent'
    # Check if column exists to avoid errors if dataframe is empty or different
    if "Tipus Avís" in row:
        if row["Tipus Avís"] == "25%":
            color = 'background-color: #ffcccc' # Red tint
        elif row["Tipus Avís"] == "15%":
            color = 'background-color: #fff4cc' # Orange tint
    return [color] * len(row)

# --- Sidebar: Configuration & Instructions ---
with st.sidebar:
    st.title("🎓 Assistència")
    
    # Navigation
    page = st.radio("Navegació", ["Gestió d'Avisos", "Historial d'Enviats", "Configuració", "Models de Correu"])
    
    st.divider()
    
    if page == "Configuració":
        st.header("⚙️ Configuració")
        # Load stored config (Now Dict of Dicts)
        full_config = load_config()
        
        # Cycle Selector
        cycle_names = list(full_config.keys())
        selected_cycle = st.selectbox("Veure configuració del Cicle:", cycle_names)
        
        st.subheader(f"🔒 Configuració: {selected_cycle}")
        st.info("La configuració està bloquejada per seguretat.")
        
        # Read-only View
        cycle_conf = full_config.get(selected_cycle, {})
        
        if cycle_conf:
            df_view = pd.DataFrame(list(cycle_conf.items()), columns=["Assignatura", "Hores"])
            st.dataframe(df_view, use_container_width=True, hide_index=True)
        else:
            st.warning("Aquest cicle no té hores definides encara.")

    st.divider()
    st.header("ℹ️ Instruccions")
    st.markdown("""
    1. **Gestió**: Puja l'Excel per veure nous avisos.
    2. **Historial**: Consulta avisos ja enviats.
    3. **Config**: Revisa les hores per cicle.
    """)

# --- Main Area ---

# HELPER: History Logic (MIGRATED TO FIRESTORE)
# HISTORY_FILE = 'warnings_history.json' # LEGACY

import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase (Singleton)
# Initialize Firebase (Singleton)
db = None # Default to None
try:
    if not firebase_admin._apps:
        secrets_found = False
        try:
            if "firebase" in st.secrets:
                cred = credentials.Certificate(dict(st.secrets["firebase"]))
                firebase_admin.initialize_app(cred)
                secrets_found = True
        except FileNotFoundError:
            pass # Expected locally
        except Exception:
            pass # Other errors, try fallback

        if not secrets_found and os.path.exists('serviceAccountKey.json'):
            cred = credentials.Certificate('serviceAccountKey.json')
            firebase_admin.initialize_app(cred)
        elif not secrets_found:
            st.error("❌ No s'han trobat credencials de Firebase (Secrets o serviceAccountKey.json).")
            st.stop()

    # Initialize Client only if app is initialized
    db = firestore.client()

except Exception as e:
    st.error(f"Error inicialitzant Firebase: {e}")
    st.stop() # Stop execution on error

COLLECTION_NAME = 'attendance_warnings'

def load_history():
    """Fetches all warnings from Firestore."""
    try:
        # st.toast("⏳ Carregant historial...", icon="🔄") # Debug info
        docs = db.collection(COLLECTION_NAME).stream()
        history = {}
        for doc in docs:
            history[doc.id] = doc.to_dict()
        return history
    except Exception as e:
        st.error(f"Error detallat carregant Firebase: {e}")
        return {}

def save_history(hist_updates):
    """
    Saves updates to Firestore.
    Accepts a dictionary of updates {id: data}.
    Note: We change the signature slightly to avoid re-uploading the whole DB.
    But for compatibility with existing code calling save_history(full_history),
    we can adapt.
    """
    try:
        # If passed the Full History dict, we might want to batch update or just set
        # Since the app code passes the WHOLE history object, iterating it all is inefficient
        # but safe for migration.
        # OPTIMIZATION: In the app logic, we identify 'changes_made'.
        # However, for now, let's just update the specific keys if possible or
        # iterating is fine for small datasets.
        
        # ACTUALLY: The app code updates the 'history' local dict and calls save_history(history).
        # We should ideally only update the modified ones.
        # But to minimal code impact, we can just iterate.
        
        batch = db.batch()
        count = 0
        for wid, data in hist_updates.items():
            doc_ref = db.collection(COLLECTION_NAME).document(wid)
            batch.set(doc_ref, data)
            count += 1
            if count == 400: # Batch limit is 500
                batch.commit()
                batch = db.batch()
                count = 0
        if count > 0:
            batch.commit()
            
    except Exception as e:
        st.error(f"Error guardant a Firebase: {e}")

if page == "Historial d'Enviats":
    st.header("📨 Historial d'Avisos Enviats")
    history = load_history()
    
    if history:
        # Convert Dict to DataFrame
        # Filter only Notified=True
        # We need the KEY (ID) to recover data for legacy entries
        sent_items = []
        for key, val in history.items():
            if val.get('notified', False):
                # Backfill logic removed/simplified as Firestore data should be clean?
                # Keeping it just in case of mixed data
                if val.get('type', '-') == '-' or val.get('type') is None:
                    try:
                        parts = key.rsplit('_', 1) 
                        if len(parts) == 2:
                            val['type'] = parts[1]
                    except:
                        pass
                
                # Flatten for DataFrame
                item = val.copy()
                item['id'] = key
                sent_items.append(item)
        
        if sent_items:
            df_hist = pd.DataFrame(sent_items)
            
            # Ensure columns exist
            check_cols = ["last_update", "student", "subject", "cycle", "group", "pct", "type"]
            for c in check_cols:
                if c not in df_hist.columns:
                    df_hist[c] = "-"
            
            # Fill NaNs just in case
            df_hist = df_hist.fillna("-")
            
            # Display
            st.markdown(f"**Total Enviats:** {len(df_hist)}")
            
            # Reorder
            cols_show = ["last_update", "student", "group", "subject", "cycle", "pct", "type"]
            cols_map = {
                "last_update": "Data Enviament",
                "student": "Alumne",
                "group": "Grup",
                "subject": "Assignatura",
                "cycle": "Cicle",
                "pct": "% Assistència",
                "type": "Tipus Avís"
            }
            
            df_display = df_hist[cols_show].rename(columns=cols_map)
            # Sort by Date Desc
            df_display = df_display.sort_values(by="Data Enviament", ascending=False)
            
            
            # Filters for History
            # Row 1: Structural Filters (Cycle, Group)
            col1, col2 = st.columns(2)
            with col1:
                hist_cycle_filter = st.multiselect("Filtrar per Cicle", sorted(df_display["Cicle"].unique().astype(str)))
            with col2:
                hist_group_filter = st.multiselect("Filtrar per Grup", sorted(df_display["Grup"].unique().astype(str)))
            
            # Row 2: Specific Filters (Subject, Student)
            col3, col4 = st.columns(2)
            with col3:
                hist_mod_filter = st.multiselect("Filtrar per Assignatura", sorted(df_display["Assignatura"].unique().astype(str)))
            with col4:
                hist_stud_filter = st.multiselect("Filtrar per Alumne", sorted(df_display["Alumne"].unique().astype(str)))
                
            # Apply Filters
            if hist_cycle_filter:
                df_display = df_display[df_display["Cicle"].isin(hist_cycle_filter)]
            if hist_group_filter:
                df_display = df_display[df_display["Grup"].isin(hist_group_filter)]
            if hist_mod_filter:
                df_display = df_display[df_display["Assignatura"].isin(hist_mod_filter)]
            if hist_stud_filter:
                df_display = df_display[df_display["Alumne"].isin(hist_stud_filter)]
            
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            st.info("💡 Nota: Les dades ara estan sincronitzades amb el núvol (Firebase).")
            
        else:
            st.info("No hi ha cap avís marcat com a enviat a l'historial.")
    else:
        st.info("L'historial està buit (Firebase).")

elif page == "Models de Correu":
    st.header("📧 Models de Correu")
    st.markdown("A continuació es mostren els plantilles de correu que s'envien automàticament segons l'etapa educativa i el tipus d'avís.")

    tab_eso, tab_batx, tab_pfi, tab_fp = st.tabs(["ESO (3r i 4t)", "Batxillerat", "PFI", "Cicles Formatius (FP)"])

    with tab_eso:
        st.subheader("Ensenyament Secundari Obligatori (ESO)")
        
        st.markdown("### 🟡 Avís 15% (Seguiment)")
        st.code("""Assumpte: Avís de seguiment d'assistència (15%) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Us informem que l'alumne ha assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

Aquesta etapa educativa és obligatòria i l'assistència és fonamental per al seguiment del curs. Us recomanem revisar la situació per evitar superar els límits que activarien mètodes de seguiment més estrictes.

Atentament,

Equip docent""", language="text")

        st.markdown("### 🔴 Avís 25% (Protocol Absentisme)")
        st.code("""Assumpte: Avís important d'absentisme escolar (25%) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Us informem que l'alumne ha superat el 25% de faltes d'assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

Recordem que l'Ensenyament Secundari Obligatori (ESO) requereix una assistència continuada. La reiteració en les faltes d'assistència sense justificar pot derivar en l'activació del protocol d'absentisme escolar, la qual cosa podria comportar la intervenció dels serveis socials o educatius competents per garantir el dret a l'escolaritat.

Us preguem que justifiqueu les absències pendents i assegureu l'assistència regular a partir d'ara.

Atentament,

Equip docent""", language="text")

    with tab_batx:
        st.subheader("Batxillerat")
        
        st.markdown("### 🟡 Avís 15% (Preventiu)")
        st.code("""Assumpte: Avís per faltes d'assistència (15%) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Us informem que l'alumne ha assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

L'assistència a classe és fonamental per al seguiment del curs. Recordem que superar el 25% de faltes implica la pèrdua del dret a l'avaluació contínua de la primera avaluació.

Us preguem que reviseu la situació.

Atentament,

Equip docent""", language="text")

        st.markdown("### 🔴 Avís 25% (Pèrdua Avaluació Contínua 1a Av.)")
        st.code("""Assumpte: Comunicació pèrdua dret a l'avaluació contínua (1a avaluació) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Mitjançant la present us comuniquem que l'alumne ha superat el 25% de faltes d'assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

Això implica la pèrdua del dret a l'avaluació contínua de la primera avaluació de totes les matèries.

Podràs recuperar l'avaluació segons els mecanismes de recuperació establerts pel departament corresponent. Per a qualsevol aclariment, pots adreçar-te al professorat de la matèria o al tutor/a.

Atentament,

Equip docent""", language="text")

    with tab_pfi:
        st.subheader("PFI (Programes de Formació i Inserció)")

        st.markdown("### 🟡 Avís 15% (Preventiu)")
        st.code("""Assumpte: Avís per faltes d'assistència (15%) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Segons el registre d'assistència del centre, has assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

Et recordem que superar el 25% de faltes implica la pèrdua del dret a l'avaluació en 1a convocatòria.

Et demanem que revisis la teva situació i milloris l'assistència. Si ho consideres oportú, posa't en contacte amb el tutor/a.

Atentament,

Equip docent""", language="text")

        st.markdown("### 🔴 Avís 25% (Pèrdua Avaluació 1a Convocatòria)")
        st.code("""Assumpte: Comunicació pèrdua dret a 1a convocatòria per faltes (25%) - Còmput Global

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Segons el registre d’assistència del centre, has superat el 25% de faltes d’assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

D’acord amb la normativa vigent, això implica la pèrdua del dret a l’avaluació en 1a convocatòria.

Podràs acollir-te a la 2a convocatòria en les condicions que fixa la normativa del centre. Per a qualsevol aclariment, pots adreçar-te al/la tutor/a.

Atentament,

Equip docent""", language="text")

    with tab_fp:
        st.subheader("Formació Professional (Cicles Formatius)")
        
        st.markdown("### 🟡 Avís 15% (Preventiu)")
        st.code("""Assumpte: Avís per faltes d'assistència (primer avís) - [Assignatura]

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Segons el registre d'assistència del centre, has assolit un 15% de faltes d'assistència al mòdul [Assignatura].

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

Et recordem que, d'acord amb el Reial decret 659/2023, superar el 25% de faltes implica la pèrdua del dret a l'avaluació en 1a convocatòria.

Et demanem que revisis la teva situació i milloris l'assistència. Si ho consideres oportú, posa't en contacte amb el professorat o el tutor/a.

Atentament,

Equip docent""", language="text")

        st.markdown("### 🔴 Avís 25% (Pèrdua Avaluació 1a Convocatòria)")
        st.code("""Assumpte: Comunicació pèrdua dret a 1a convocatòria per faltes (25%)

Benvolgut/da,

Alumne: [Nom de l'Alumne]
Grup: [Grup]

Segons el registre d’assistència del centre, has superat el 25% de faltes d’assistència al mòdul [Assignatura], el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: [X] h
- Hores Retards: [X] h
- Percentatge Actual: [X]%

D’acord amb el que estableix el Reial decret 659/2023, de 18 de juliol, pel qual es desenvolupa l’ordenació del Sistema de Formació Professional, això implica la pèrdua del dret a l’avaluació en 1a convocatòria d’aquest mòdul.

Podràs acollir-te a la 2a convocatòria en les condicions que fixa la programació del mòdul i la normativa del centre. Per a qualsevol aclariment o per resoldre dubtes, pots adreçar-te al professorat del mòdul, al/la tutor/a o al cap d’estudis.

Atentament,

Equip docent""", language="text")

elif page == "Gestió d'Avisos":
    st.header("📊 Gestió d'Alumnes i Avisos")
    
    # Load config needed for processing
    full_config = load_config()

    uploaded_file = st.file_uploader("Puja el fitxer Excel (.xlsx)", type=["xlsx", "xls"])
    
    if uploaded_file:
        try:
            # Helper to generate Gmail Web Link (More robust than mailto for Web users)
            import urllib.parse
            
            def create_gmail_link(row):
                is_25 = row['Tipus Avís'] == "25%"
                cycle = row.get('Cicle (Detectat)', '')
                is_eso = cycle in ["3 ESO", "4 ESO"]

                is_batx = "BATX" in cycle
                is_pfi = "PFI" in cycle

                if is_eso:
                    # --- TEMPLATES ESO ---
                    if is_25:
                        subject = f"Avís important d'absentisme escolar (25%) - Còmput Global"
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Us informem que l'alumne ha superat el 25% de faltes d'assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

Recordem que l'Ensenyament Secundari Obligatori (ESO) requereix una assistència continuada. La reiteració en les faltes d'assistència sense justificar pot derivar en l'activació del protocol d'absentisme escolar, la qual cosa podria comportar la intervenció dels serveis socials o educatius competents per garantir el dret a l'escolaritat.

Us preguem que justifiqueu les absències pendents i assegureu l'assistència regular a partir d'ara.

Atentament,

Equip docent"""
                    else:
                        subject = f"Avís de seguiment d'assistència (15%) - Còmput Global"
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Us informem que l'alumne ha assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

Aquesta etapa educativa és obligatòria i l'assistència és fonamental per al seguiment del curs. Us recomanem revisar la situació per evitar superar els límits que activarien mètodes de seguiment més estrictes.

Atentament,

Equip docent"""

                elif is_batx:
                    # --- TEMPLATES BATXILLERAT ---
                    if is_25:
                        subject = f"Comunicació pèrdua dret a l'avaluació contínua (1a avaluació) - Còmput Global"
                        
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Mitjançant la present us comuniquem que l'alumne ha superat el 25% de faltes d'assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

Això implica la pèrdua del dret a l'avaluació contínua de la primera avaluació de totes les matèries.

Podràs recuperar l'avaluació segons els mecanismes de recuperació establerts pel departament corresponent. Per a qualsevol aclariment, pots adreçar-te al professorat de la matèria o al tutor/a.

Atentament,

Equip docent"""

                    else:
                        # --- TEMPLATE 15% BATX ---
                        subject = f"Avís per faltes d'assistència (15%) - Còmput Global"
                        
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Us informem que l'alumne ha assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

L'assistència a classe és fonamental per al seguiment del curs. Recordem que superar el 25% de faltes implica la pèrdua del dret a l'avaluació contínua de la primera avaluació.

Us preguem que reviseu la situació.

Atentament,

Equip docent"""

                elif is_pfi:
                    # --- TEMPLATES PFI (Global + FP Consequences) ---
                    if is_25:
                        subject = f"Comunicació pèrdua dret a 1a convocatòria per faltes (25%) - Còmput Global"
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Segons el registre d’assistència del centre, has superat el 25% de faltes d’assistència en el còmput global del curs, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

D’acord amb la normativa vigent, això implica la pèrdua del dret a l’avaluació en 1a convocatòria.

Podràs acollir-te a la 2a convocatòria en les condicions que fixa la normativa del centre. Per a qualsevol aclariment, pots adreçar-te al/la tutor/a.

Atentament,

Equip docent"""
                    else:
                        subject = f"Avís per faltes d'assistència (15%) - Còmput Global"
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Segons el registre d'assistència del centre, has assolit un 15% de faltes d'assistència en el còmput global del curs.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

Et recordem que superar el 25% de faltes implica la pèrdua del dret a l'avaluació en 1a convocatòria.

Et demanem que revisis la teva situació i milloris l'assistència. Si ho consideres oportú, posa't en contacte amb el tutor/a.

Atentament,

Equip docent"""

                else:
                    # --- TEMPLATES STANDARD (FP) ---
                    if is_25:
                        # --- TEMPLATE 25% (PÈRDUA AVALUACIÓ) ---
                        subject = "Comunicació pèrdua dret a 1a convocatòria per faltes (25%)"
                        
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Segons el registre d’assistència del centre, has superat el 25% de faltes d’assistència al mòdul {row['Assignatura']}, el màxim permès.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

D’acord amb el que estableix el Reial decret 659/2023, de 18 de juliol, pel qual es desenvolupa l’ordenació del Sistema de Formació Professional, això implica la pèrdua del dret a l’avaluació en 1a convocatòria d’aquest mòdul.

Podràs acollir-te a la 2a convocatòria en les condicions que fixa la programació del mòdul i la normativa del centre. Per a qualsevol aclariment o per resoldre dubtes, pots adreçar-te al professorat del mòdul, al/la tutor/a o al cap d’estudis.

Atentament,

Equip docent"""

                    else:
                        # --- TEMPLATE 15% (AVÍS PREVENTIU) ---
                        subject = f"Avís per faltes d'assistència (primer avís) - {row['Assignatura']}"
                        
                        body = f"""Benvolgut/da,

Alumne: {row['Alumne']}
Grup: {row['Grup']}

Segons el registre d'assistència del centre, has assolit un 15% de faltes d'assistència al mòdul {row['Assignatura']}.

RESUM DE LA SITUACIÓ ACTUAL:
- Hores Faltes Reals: {row['Hores Faltes (Reals)']} h
- Hores Retards: {row['Hores Retards']} h
- Percentatge Actual: {row['% Actual']}

Et recordem que, d'acord amb el Reial decret 659/2023, superar el 25% de faltes implica la pèrdua del dret a l'avaluació en 1a convocatòria.

Et demanem que revisis la teva situació i milloris l'assistència. Si ho consideres oportú, posa't en contacte amb el professorat o el tutor/a.

Atentament,

Equip docent"""
                
                # Construct Gmail URL
                params = {
                    "view": "cm",
                    "fs": "1",
                    "su": subject,
                    "body": body
                }
                query_string = urllib.parse.urlencode(params)
                return f"https://mail.google.com/mail/?{query_string}"

            # Load Data - Dynamic Header Search
            temp_df = pd.read_excel(uploaded_file, header=None)
            
            header_row_idx = None
            for i, row in temp_df.iterrows():
                row_str = row.astype(str).str.strip().tolist()
                if "Alumne/a" in row_str and "Assignatura" in row_str:
                    header_row_idx = i
                    break
            
            if header_row_idx is None:
                st.error("❌ No s'ha trobat la fila de capçalera (Alumne/a, Assignatura...).")
                st.stop()
                
            # Re-read with correct header
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, header=header_row_idx)
            df.columns = df.columns.str.strip()
            
            required_cols = ['Alumne/a', 'Tipus', 'Hora', 'Assignatura']
            missing = [c for c in required_cols if c not in df.columns]
            
            if missing:
                 st.error(f"❌ Falten columnes obligatòries a l'Excel: {', '.join(missing)}")
            else:
                # 1. Detect Subjects
                unique_subjects = sorted(df['Assignatura'].dropna().unique())
                
                # Identify columns
                col_map = {c: c for c in df.columns}
                # Handle potential variations in column names if needed, but 'Grup (incidència)' is expected
                grup_col = next((c for c in df.columns if "Grup" in c), None)
    
                if not grup_col:
                    st.error("⚠️ No s'ha trobat la columna 'Grup' (o 'Grup (incidència)'). No es pot determinar el cicle.")
                    st.stop()
                
                # PROCESS DATA
                st.divider()
                
                # --- AUTO-OPEN GMAIL LOGIC (Post-Rerun) ---
                if "auto_open_gmail" in st.session_state:
                    url_to_open = st.session_state["auto_open_gmail"]
                    
                    # Use Python's native webbrowser module (Works perfectly for local apps)
                    import webbrowser
                    try:
                        webbrowser.open_new_tab(url_to_open)
                        st.toast("📧 Gmail obert correctament!", icon="🚀")
                    except Exception as e:
                        st.error(f"No s'ha pogut obrir el navegador: {e}")
                        
                    # Clear state
                    del st.session_state["auto_open_gmail"]

                with st.spinner("Analitzant dades..."):
                    # Filter Absences and Delays
                    # Codes: F, FJ, FJP (Absences); R, RJ, RJP (Delays)
                    absence_codes = ['F', 'FJ', 'FJP']
                    delay_codes = ['R', 'RJ', 'RJP']
                    valid_types = absence_codes + delay_codes
                    
                    # Filter rows with valid types
                    # Use str.strip() to be safe
                    mask = df['Tipus'].astype(str).str.strip().isin(valid_types)
                    records = df[mask].copy()
                    
                    # Calculate duration
                    records['Durada'] = records['Hora'].apply(calculate_duration)
                    records['Grup_Clean'] = records[grup_col].astype(str).str.strip()
                    records['Tipus_Clean'] = records['Tipus'].astype(str).str.strip()
                    
                    # --- NORMALIZATION FOR EB/PER Project Modules ---
                    # Request: Merge "Mòdul projecte" into "Projecte intermodular" for groups containing "EB" or "PER"
                    def normalize_project_module(row):
                        grup = row['Grup_Clean']
                        subj = row['Assignatura']
                        
                        if "EB" in grup or "PER" in grup:
                            if subj == "Mòdul projecte":
                                return "Projecte intermodular"
                        return subj

                    records['Assignatura'] = records.apply(normalize_project_module, axis=1)
                    # -----------------------------------------------
                    
                    # Group by key fields
                    # We need to sum durations separately for Absences and Delays
                    
                    # Helper to categorize
                    def get_category(code):
                        if code in absence_codes: return 'Absence'
                        if code in delay_codes: return 'Delay'
                        return 'Other'
    
                    records['Category'] = records['Tipus_Clean'].apply(get_category)
    
                    # Group by Student, Subject, Group, Category
                    grouped = records.groupby(['Alumne/a', 'Assignatura', 'Grup_Clean', 'Category'])['Durada'].sum().reset_index()
                    
                    # Pivot or iterate to combine
                    # It's easier to iterate unique (Student, Subject, Group) combinations
                    unique_combinations = records[['Alumne/a', 'Assignatura', 'Grup_Clean']].drop_duplicates()
                    
                    warnings = []
                    summary_data = []
                    processing_date = datetime.now().strftime("%d/%m/%Y")
                    
                    # Known Cycle Keys
                    known_cycles = list(full_config.keys())
                    
                    # PFI & ESO/BATX Global Configuration & Buffer
                    # We treat these as "Global" cycles where attendance is calculated on TOTAL hours, not per module.
                    global_cycles = ["PFIPER", "PFICOM", "3 ESO", "4 ESO", "1 BATX", "2 BATX"]
                    
                    global_total_hours = {}
                    for g_cycle in global_cycles:
                        # Hardcoded defaults if missing in JSON, or specific overrides
                        if g_cycle == "3 ESO":
                            global_total_hours[g_cycle] = 1080
                        elif g_cycle == "4 ESO":
                            global_total_hours[g_cycle] = 1080
                        elif g_cycle == "1 BATX":
                             # Default if not in config, though it should be. 
                             # If in config, sum values.
                             if g_cycle in full_config:
                                 global_total_hours[g_cycle] = sum(full_config[g_cycle].values())
                             else:
                                 global_total_hours[g_cycle] = 1020 # Typical fallback
                        elif g_cycle == "2 BATX":
                            global_total_hours[g_cycle] = 1020
                        elif g_cycle in full_config:
                            # For PFIs or others defined in JSON
                            global_total_hours[g_cycle] = sum(full_config[g_cycle].values())
                    
                    # Buffer for Global aggregation: {(Student, Group): {'abs': 0, 'delay': 0, 'cycle': ''}}
                    global_buffer = {}
                    
                    # Pre-calculate a normalized config map for each cycle
                    cycle_normalized_maps = {}
                    import re
                    import unicodedata
                    
                    for c_code, c_conf in full_config.items():
                        cycle_normalized_maps[c_code] = {}
                        for subj_key, hrs in c_conf.items():
                            # Strip leading code like "0633. ", "MP1226 ", "MP3060_"
                            # Pattern: Start, optional chars, DIGIT(s), optional chars, (dot OR underscore OR dash OR space)
                            # This also handles "MP3060_Preparació" -> "Preparació"
                            norm_name = re.sub(r'^[^\s]*\d+[^\s]*[\._\-\s]\s*', '', subj_key).strip().lower()
                            # Normalize apostrophes
                            norm_name = norm_name.replace("’", "'").replace("‘", "'").replace("´", "'").replace("`", "'")
                            # Normalize Unicode to NFC
                            norm_name = unicodedata.normalize('NFC', norm_name)
                            cycle_normalized_maps[c_code][norm_name] = hrs
    
                    for idx, row in unique_combinations.iterrows():
                        student = row['Alumne/a']
                        subject = row['Assignatura']
                        group_name = row['Grup_Clean']
                        
                        # Get hours for this combo
                        # Filter from grouped
                        combo_stats = grouped[
                            (grouped['Alumne/a'] == student) & 
                            (grouped['Assignatura'] == subject) & 
                            (grouped['Grup_Clean'] == group_name)
                        ]
                        
                        abs_hours = combo_stats[combo_stats['Category'] == 'Absence']['Durada'].sum()
                        delay_hours = combo_stats[combo_stats['Category'] == 'Delay']['Durada'].sum()
                        
                        # Formula: Effective = Absences + (Delays / 3)
                        effective_hours = abs_hours + (delay_hours / 3.0)
                        
                        # 1. Determine Cycle from Group Name
                        matched_cycle = None
                        
                        # Special check for ESO and BATX
                        if "3" in group_name and "ESO" in group_name:
                            matched_cycle = "3 ESO"
                        elif "4" in group_name and "ESO" in group_name:
                            matched_cycle = "4 ESO"
                        elif "1" in group_name and "BATX" in group_name:
                            matched_cycle = "1 BATX"
                        elif "2" in group_name and "BATX" in group_name:
                            matched_cycle = "2 BATX"
                        else:
                            for c_code in sorted(known_cycles, key=len, reverse=True):
                                if c_code in group_name:
                                    matched_cycle = c_code
                                    break
                        
                        # --- SPECIAL LOGIC FOR GLOBAL CYCLES (PFI, ESO, BATX) ---
                        if matched_cycle in global_cycles:
                            # Aggregate to buffer
                            key = (student, group_name)
                            if key not in global_buffer:
                                global_buffer[key] = {'abs': 0, 'delay': 0, 'cycle': matched_cycle}
                            
                            global_buffer[key]['abs'] += abs_hours
                            global_buffer[key]['delay'] += delay_hours
                        
                        total_module_hours = 0
                        if matched_cycle:
                            # 2. Look up hours in that cycle's normalized config
                            excel_subj_norm = str(subject).strip().lower()
                            # Normalize apostrophes for lookup too
                            excel_subj_norm = excel_subj_norm.replace("’", "'").replace("‘", "'").replace("´", "'").replace("`", "'")
                            # Normalize Unicode to NFC
                            excel_subj_norm = unicodedata.normalize('NFC', excel_subj_norm)
                            
                            cycle_map = cycle_normalized_maps.get(matched_cycle, {})
                            
                            if excel_subj_norm in cycle_map:
                                total_module_hours = cycle_map[excel_subj_norm]
                            else:
                                # Fuzzy fallback
                                for cfg_name, cfg_hours in cycle_map.items():
                                    if cfg_name in excel_subj_norm or excel_subj_norm in cfg_name:
                                        total_module_hours = cfg_hours
                                        break
                        
                        # Prepare data for summary (regardless of warning)
                        pct = 0
                        if total_module_hours > 0:
                            pct = (effective_hours / total_module_hours) * 100
                        
                        summary_item = {
                            "Alumne": student,
                            "Cicle": matched_cycle if matched_cycle else "Desconegut",
                            "Grup": group_name,
                            "Assignatura": subject,
                            "Hores Totals": total_module_hours,
                            "Hores Faltes": round(abs_hours, 2),
                            "Hores Retards": round(delay_hours, 2),
                            "Hores Efectives": round(effective_hours, 2),
                            "% Assistència": f"{round(pct, 1)}%"
                        }
                        summary_data.append(summary_item)
    
                        # Check Thresholds (SKIP for Global Cycles)
                        if matched_cycle in global_cycles:
                            continue

                        if total_module_hours > 0:
                            warning_type = None
                            if pct >= 25:
                                warning_type = "25%"
                            elif pct >= 15:
                                warning_type = "15%"
                                
                            if warning_type:
                                warnings.append({
                                    "Data Avís": processing_date,
                                    "Alumne": student,
                                    "Grup": group_name,
                                    "Assignatura": subject,
                                    "Cicle (Detectat)": matched_cycle,
                                    "Hores Faltes (Reals)": round(abs_hours, 2),
                                    "Hores Retards": round(delay_hours, 2),
                                    "Hores Efectives (F + R/3)": round(effective_hours, 2),
                                    "Hores Totals Mòdul": total_module_hours,
                                    "% Actual": f"{round(pct, 1)}%",
                                    "Tipus Avís": warning_type
                                })
                        else:
                            # Optional: Log missing config
                            pass
                    
                    # --- PROCESS GLOBAL BUFFER (Global Warnings) ---
                    for (student, group_name), stats in global_buffer.items():
                        c_code = stats['cycle']
                        total_cycle_hours = global_total_hours.get(c_code, 0)
                        
                        abs_hours = stats['abs']
                        delay_hours = stats['delay']
                        effective_hours = abs_hours + (delay_hours / 3.0)
                        
                        pct = 0
                        if total_cycle_hours > 0:
                            pct = (effective_hours / total_cycle_hours) * 100
                            
                        # Add GLOBAL line to Summary
                        summary_data.append({
                            "Alumne": student,
                            "Assignatura": "GLOBAL (Còmput Total)",
                            "Grup": group_name,
                            "Cicle": c_code,
                            "Hores Totals": total_cycle_hours,
                            "Hores Faltes": round(abs_hours, 2),
                            "Hores Retards": round(delay_hours, 2),
                            "Hores Efectives": round(effective_hours, 2),
                            "% Assistència": f"{round(pct, 2)}%"
                        })
                        
                        # Generate Warning
                        if pct >= 15:
                            warning_type = ""
                            if pct >= 25:
                                warning_type = "25%"
                            elif pct >= 15:
                                warning_type = "15%"
                                
                            if warning_type:
                                warnings.append({
                                    "Data Avís": processing_date,
                                    "Alumne": student,
                                    "Grup": group_name,
                                    "Assignatura": "GLOBAL (Còmput Total)",
                                    "Cicle (Detectat)": c_code,
                                    "Hores Faltes (Reals)": round(abs_hours, 2),
                                    "Hores Retards": round(delay_hours, 2),
                                    "Hores Efectives (F + R/3)": round(effective_hours, 2),
                                    "Hores Totals Mòdul": total_cycle_hours,
                                    "% Actual": f"{round(pct, 1)}%",
                                    "Tipus Avís": warning_type
                                })

                    # --- RESULTS DISPLAY ---
                    
                    # 1. SUMMARY REPORT
                    if summary_data:
                        with st.expander("📊 Resum de Faltes i Retards (Tots els alumnes)", expanded=False):
                            df_summary = pd.DataFrame(summary_data)
                            # Reorder cols
                            cols = ["Alumne", "Cicle", "Grup", "Assignatura", "Hores Totals", "Hores Faltes", "Hores Retards", "Hores Efectives", "% Assistència"]
                            st.dataframe(df_summary[cols], use_container_width=True)
    
                    # 2. WARNINGS
                    st.subheader("⚠️ Avisos Generats (>15% i >25%)")
                    if warnings:
                        # --- Persistence Logic ---
                        # Uses GLOBAL load_history/save_history (Firebase)
    
                        history = load_history()
                        
                        # Add unique ID for tracking
                        # ID = Student + Module + Type (e.g., "John Doe_Math_15%")
                        # We store: {ID: {notified: True, date: ...}}
                        
                        res_df = pd.DataFrame(warnings)
                        
                        # Create IDs (Composite Key)
                        res_df['Avís ID'] = res_df['Alumne'] + "_" + res_df['Assignatura'] + "_" + res_df['Tipus Avís']
                        
                        # Map current history status
                        res_df['Avís Enviat'] = res_df['Avís ID'].apply(lambda x: history.get(x, {}).get('notified', False))
                        res_df['Data Enviament'] = res_df['Avís ID'].apply(lambda x: history.get(x, {}).get('last_update', ''))
                        
                        # Generate Gmail Links
                        res_df['Link Gmail'] = res_df.apply(create_gmail_link, axis=1)
    
                        # --- FILTERS ---
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            # Filter by Cycle/Group
                            all_groups = sorted(res_df['Grup'].unique())
                            sel_groups = st.multiselect("Filtrar per Grup", all_groups)
                        
                        with col2:
                            # Filter by Student
                            # If group is selected, filter students
                            if sel_groups:
                                avail_students = sorted(res_df[res_df['Grup'].isin(sel_groups)]['Alumne'].unique())
                            else:
                                avail_students = sorted(res_df['Alumne'].unique())
                            sel_students = st.multiselect("Filtrar per Alumne", avail_students)
                            
                        with col3:
                             # Filter by Subject
                            avail_subjs = sorted(res_df['Assignatura'].unique())
                            sel_subjs = st.multiselect("Filtrar per Assignatura", avail_subjs)
                        
                        # Apply Filters
                        filtered_df = res_df.copy()
                        if sel_groups:
                            filtered_df = filtered_df[filtered_df['Grup'].isin(sel_groups)]
                        if sel_students:
                            filtered_df = filtered_df[filtered_df['Alumne'].isin(sel_students)]
                        if sel_subjs:
                            filtered_df = filtered_df[filtered_df['Assignatura'].isin(sel_subjs)]
    
                        st.markdown(f"**Mostrant {len(filtered_df)} avisos de {len(res_df)} totals.**")
                        
                        # Reorder columns for clarity
                        # Checkbox FIRST, then Timestamp
                        # We HIDE 'Link Gmail' to force usage of Checkbox for "Send + Mark" workflow
                        cols_order = [
                            "Avís Enviat", 
                            "Data Enviament",
                            "Data Avís", "Alumne", "Grup", "Assignatura", "Cicle (Detectat)",
                            "Hores Totals Mòdul", 
                            "Hores Faltes (Reals)", "Hores Retards", "Hores Efectives (F + R/3)",
                            "% Actual", "Tipus Avís",
                            "Avís ID",
                            "Link Gmail" # Kept in df for logic, but will be hidden in editor
                        ]
                        
                        # Columns to actually show in editor
                        # We exclude 'Link Gmail' explicitly from user view to avoid confusion
                        show_cols = [c for c in cols_order if c != "Link Gmail" and c in filtered_df.columns]
                        
                        # Use Data Editor for interactivity on the FILTERED dataframe
                        edited_df = st.data_editor(
                            filtered_df[show_cols], 
                            column_config={
                                "Avís Enviat": st.column_config.CheckboxColumn(
                                    "✅ Enviat / 📧 Enviar",
                                    help="Marca aquesta casella per GUARDAR l'estat i OBRIR automàticament el correu a Gmail.",
                                    default=False,
                                ),
                                "Data Enviament": st.column_config.TextColumn(
                                    "Data Enviament",
                                    disabled=True # Read-only
                                )
                            },
                            disabled=[c for c in show_cols if c != "Avís Enviat"], # Only checkbox editable
                            use_container_width=True,
                            hide_index=True,
                            key="warnings_editor"
                        )
                        
                        # Detect Changes and Save
                        # IMPORTANT: We must compare against filtered_df processing, but update HISTORY globally
                        
                        # We need to reconstruct the full df to compare, or just iterate changes
                        # Since we only edited 'Avís Enviat', we can track differences
                        
                        # Logic to detect changes in 'Avís Enviat' specifically
                        # We iterate the edited_df (which is a subset)
                        
                        # To correctly map back, we rely on 'Avís ID' being present in show_cols
                        
                        changes_made = False
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # Check generic diff to avoid unnecessary loops if nothing changed
                        if not filtered_df[show_cols].equals(edited_df):
                            # Start with empty dict for updates if we want to be clean, 
                            # but save_history(full_history) needs full dict or we adapt save_history.
                            # Our save_history implementation handles a dict of {id: data}.
                            # Since 'history' is the full dict, it's fine.
                            
                            for index, row in edited_df.iterrows():
                                wid = row['Avís ID']
                                is_checked = row['Avís Enviat']
                                
                                # Check against stored
                                stored_info = history.get(wid, {})
                                stored_status = stored_info.get('notified', False)
                                
                                if is_checked != stored_status:
                                    # Update history
                                    new_entry = {
                                        "notified": is_checked,
                                        "student": row['Alumne'],
                                        "subject": row['Assignatura'],
                                        "group": row['Grup'],
                                        "cycle": row['Cicle (Detectat)'],
                                        "pct": row['% Actual'],
                                        "type": row['Tipus Avís'],
                                        "last_update": stored_info.get('last_update', '')
                                    }
                                    
                                    if is_checked:
                                        new_entry["last_update"] = current_time
                                        # TRIGGER AUTO-OPEN GMAIL
                                        # Regenerate link for this specific row
                                        # We need to find the original row data including hidden cols like 'Link Gmail' logic
                                        # But 'Link Gmail' is computable from row data we have in edited_df? 
                                        # No, create_gmail_link needs 'Assignatura', 'Tipus Avís', etc. 
                                        # Fortunately edited_df has these columns (just read-only).
                                        gmail_link = create_gmail_link(row)
                                        st.session_state["auto_open_gmail"] = gmail_link
                                    else:
                                        new_entry["last_update"] = ""
                                        
                                    history[wid] = new_entry
                                    # We can optimize by only passing 'new_entry' to save_history if we change its signature
                                    # but current save_history iterates the whole dict provided.
                                    # Let's fix save_history call to be efficient or accept that it iterates.
                                    # Actually, let's just pass {wid: new_entry} to save_history?
                                    # No, let's keep it simple: Pass only UPDATES
                                    save_history({wid: new_entry})
                                    changes_made = True
                            
                            if changes_made:
                                st.rerun()
    
                        # --- ROBUST EMAIL ACTION ---
                        st.divider()
                        st.subheader("📧 Generador de Correus")
                        st.info("Si prefereixes enviar-ho manualment sense fer servir la taula:")
                        
                        # Create a list of options: "Student - Subject (Type)"
                        # We need to map back to the dataframe row
                        email_options = filtered_df.apply(lambda x: f"{x['Alumne']} - {x['Assignatura']} ({x['Tipus Avís']})", axis=1).tolist()
                        
                        if email_options:
                            selected_option = st.selectbox("Selecciona l'alumne per enviar l'avís:", email_options)
                            
                            # Find the row
                            selected_row = None
                            for idx, row in filtered_df.iterrows():
                                opt_str = f"{row['Alumne']} - {row['Assignatura']} ({row['Tipus Avís']})"
                                if opt_str == selected_option:
                                    selected_row = row
                                    break
                            
                            if selected_row is not None:
                                # Logic: Button that MARKS and OPENS
                                if st.button(f"📧 Enviar i Marcar com a Enviat ({selected_row['Alumne']})", type="primary", use_container_width=True):
                                    # 1. Update History
                                    wid = selected_row['Avís ID']
                                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    
                                    new_entry = {
                                        "notified": True,
                                        "student": selected_row['Alumne'],
                                        "subject": selected_row['Assignatura'],
                                        "group": selected_row['Grup'],
                                        "cycle": selected_row['Cicle (Detectat)'],
                                        "pct": selected_row['% Actual'],
                                        "type": selected_row['Tipus Avís'],
                                        "last_update": current_time
                                    }
                                    history[wid] = new_entry
                                    save_history({wid: new_entry}) # Pass update only
                                    
                                    # 2. Trigger Auto Open
                                    gmail_link = create_gmail_link(selected_row)
                                    st.session_state["auto_open_gmail"] = gmail_link
                                    st.rerun()
                                    
                        else:
                            st.write("No hi ha avisos per mostrar.")

                        st.divider()
                        csv = filtered_df.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Descarregar CSV", csv, "avisos_assistencia.csv", "text/csv")
                    else:
                        st.success("✅ No s'han detectat alumnes que superin el 15% de faltes.")
                        
        except Exception as e:
            st.error(f"Error processant el fitxer: {e}")
