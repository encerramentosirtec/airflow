class configs:
    # Acessos
    login_gpm = 'bob'
    senha_gpm = '808062023'
    cookie_frotalog = 'JSESSIONID=67AD735C8F718D0B5063185773C251A2'
    
    # Acesso banco de dados
    mysql_connection = {
        'user': 'root',
        'password': '',
        #'host': '	localhost',
        'host': '192.168.3.239',
        'database': 'bobSYS',
        'raise_on_warnings': True,
    }

    # Verificação
    intervaloapagar = 'A2:N10000'
    vistoria90Dias = 'Vistoria90Dias'
    vistoria30Dias = 'Vistoria30Dias'
    meses90 = 3

    id_planilha_vistoria = '1NmCmH6EgnWZ4dzG-U3TYckYwEnwNCWpomVbNud4c_z4'
    basepcp = '1ztV6DYCUkhefULyxJTBiaAKLB_x5zAHgK9icFqBDvf4'

    # v5
    id_planilha_postagemV5 = '1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84'
    status_hektor = 'AP2:AP'
    auxiliar_hektor = 'AY2:AY'
    status_pasta = 'K2:K'
    
    # Asbuilt
    id_planilha_planejamento = '1Ogsv2IUWmqG6icE2nk7xraKqnDzXoGgDUv5wxdMEeqQ'
    id_planilha = '1WyjUGW3IP_or21BDUmi8QSf_Moa8RG53CgOyT5L_0OU'
    id_planilha2 = '1GQ5pLG2DddGrEuRJILe-3g_Rwzhg-82EkVFZnX1_we4'
    carteira_g = '1kldnvIS_w2X_N_DvOU7vDjxLrr45-ZMpYNeRjidpF0c'
    carteira_CCM = '1JcwCZQ-xZvHjbRfMlB-Je7MELSz388vSECTKHHZGL9g'
    diaC = '1JcwCZQ-xZvHjbRfMlB-Je7MELSz388vSECTKHHZGL9g'

    # Avaço GPM
    id_planilha_planejamento_conquista = '1hIaJaiPm2JNl7ogPcAu8-172flUK2uLaDPs3hyeMQ6g'
    id_planilha_planejamento_jequie = '1EruOLu5kNzq3Vn7Xj4XmgXBOlXXAFhEdmjBWS-rgvWk'
    id_planilha_planejamento_irece = '1gFStrS82U7PRX5gBd9jVvYVdkUDAYNFamNxoFATL2kc'
    id_planilha_planejamento_guanambi = '1OGQse2IeSjxfZ-MRtXFarssQegqlAsZ8ruZaKwar6rY'
    id_planilha_planejamento_lapa = '1HLsZcMjsKiqqsKZnU_osIOmKFytkNqBTxKPhdqBzcmg'
    id_planilha_planejamento_barreiras = '1-xNWYwTWVl9w-eHYGgMzFiV2TPlDCx88y82HnL2wx4U'
    id_planilha_planejamento_ibotirama = '1Fo2obLTZObf33d2vA_1GbzPzxCU1egJdX9okIF06oDo'
    unidades = ['conquista', 'guanambi', 'jequié', 'irecê', 'lapa', 'barreiras', 'ibotirama']