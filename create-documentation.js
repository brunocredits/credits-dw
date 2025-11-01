const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, HeadingLevel, 
        AlignmentType, BorderStyle, WidthType, ShadingType, LevelFormat, PageBreak, 
        UnderlineType, PageNumber, Header, Footer, TableOfContents } = require('docx');
const fs = require('fs');

// Define table borders
const tableBorder = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const cellBorders = { top: tableBorder, bottom: tableBorder, left: tableBorder, right: tableBorder };

// Define numbering for lists
const numberingConfig = [
  {
    reference: "bullet-list",
    levels: [
      { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }
    ]
  },
  {
    reference: "numbered-list-1",
    levels: [
      { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }
    ]
  }
];

const doc = new Document({
  numbering: { config: numberingConfig },
  styles: {
    default: { 
      document: { 
        run: { font: "Arial", size: 24 } // 12pt default
      } 
    },
    paragraphStyles: [
      {
        id: "Title",
        name: "Title",
        basedOn: "Normal",
        run: { size: 56, bold: true, color: "1F4E78", font: "Arial" },
        paragraph: { spacing: { before: 240, after: 240 }, alignment: AlignmentType.CENTER }
      },
      {
        id: "Heading1",
        name: "Heading 1",
        basedOn: "Normal",
        next: "Normal",
        quickFormat: true,
        run: { size: 32, bold: true, color: "1F4E78", font: "Arial" },
        paragraph: { spacing: { before: 360, after: 120 }, outlineLevel: 0 }
      },
      {
        id: "Heading2",
        name: "Heading 2",
        basedOn: "Normal",
        next: "Normal",
        quickFormat: true,
        run: { size: 28, bold: true, color: "2E5C8A", font: "Arial" },
        paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 1 }
      },
      {
        id: "Heading3",
        name: "Heading 3",
        basedOn: "Normal",
        next: "Normal",
        quickFormat: true,
        run: { size: 26, bold: true, color: "4472C4", font: "Arial" },
        paragraph: { spacing: { before: 180, after: 100 }, outlineLevel: 2 }
      },
      {
        id: "CodeBlock",
        name: "Code Block",
        basedOn: "Normal",
        run: { font: "Courier New", size: 20, color: "000000" },
        paragraph: { 
          spacing: { before: 120, after: 120 }, 
          indent: { left: 360 } 
        }
      }
    ]
  },
  sections: [{
    properties: {
      page: {
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }
      }
    },
    headers: {
      default: new Header({
        children: [
          new Paragraph({
            alignment: AlignmentType.RIGHT,
            children: [
              new TextRun({ text: "Data Warehouse Credits Brasil", size: 20, color: "666666" })
            ]
          })
        ]
      })
    },
    footers: {
      default: new Footer({
        children: [
          new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [
              new TextRun({ text: "Página ", size: 20 }),
              new TextRun({ children: [PageNumber.CURRENT] }),
              new TextRun({ text: " de ", size: 20 }),
              new TextRun({ children: [PageNumber.TOTAL_PAGES] })
            ]
          })
        ]
      })
    },
    children: [
      // Capa
      new Paragraph({
        heading: HeadingLevel.TITLE,
        children: [new TextRun("Data Warehouse Credits Brasil")]
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 120, after: 480 },
        children: [
          new TextRun({ text: "Documentação Técnica Completa", size: 28, bold: true, color: "4472C4" })
        ]
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 120 },
        children: [new TextRun({ text: "Arquitetura Medallion: Bronze → Silver → Gold", size: 24, italics: true })]
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 120 },
        children: [new TextRun({ text: "PostgreSQL 15 com Docker", size: 22 })]
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 240 },
        children: [new TextRun({ text: "Versão 1.0 | Outubro 2025", size: 22, color: "666666" })]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // Sumário
      new TableOfContents("Sumário", { hyperlink: true, headingStyleRange: "1-3" }),
      
      new Paragraph({ children: [new PageBreak()] }),

      // 1. VISÃO GERAL
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("1. Visão Geral do Projeto")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("O "),
          new TextRun({ text: "Data Warehouse Credits Brasil", bold: true }),
          new TextRun(" é uma solução de consolidação de dados que integra múltiplas fontes (sistemas internos e parceiros) utilizando a arquitetura Medallion em três camadas: Bronze, Silver e Gold. O objetivo principal é fornecer uma visão unificada e estratégica dos dados para tomada de decisões nas áreas de Vendas, Financeiro, Atendimento e Gestão.")
        ]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("1.1 Objetivos Principais")]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Consolidar dados de 9+ fontes diferentes em um único repositório")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Fornecer visão 360° do cliente integrando dados de CRM, ERP e parceiros")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Automatizar processos de ETL com Python e agendamento")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Garantir qualidade, rastreabilidade e auditoria dos dados")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Entregar views analíticas prontas para consumo em Power BI")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("1.2 Fontes de Dados")]
      }),

      new Table({
        columnWidths: [2800, 2800, 3760],
        margins: { top: 100, bottom: 100, left: 180, right: 180 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                shading: { fill: "1F4E78", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Fonte", bold: true, size: 22, color: "FFFFFF" })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                shading: { fill: "1F4E78", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Tipo", bold: true, size: 22, color: "FFFFFF" })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                shading: { fill: "1F4E78", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Dados Fornecidos", bold: true, size: 22, color: "FFFFFF" })]
                })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("CSV")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Clientes, Contratos, Produtos, Precificação")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Ploomes CRM")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Deals, Contacts, Organizations")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Omie ERP")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Notas Fiscais, Contas a Pagar/Receber")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Movidesk")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Tickets de Atendimento, SLA")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Finqi")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Consumo de Serviços, Transações")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Salesbox")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Consumo de Produtos, Uso")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Acertpix")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Transações PIX")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("SPC Brasil")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 2800, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API REST/JSON")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3760, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Consultas de Crédito, Score")] })]
              })
            ]
          })
        ]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 2. ARQUITETURA MEDALLION
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("2. Arquitetura Medallion")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("A arquitetura Medallion organiza os dados em três camadas progressivas, cada uma com responsabilidades específicas. Este modelo garante rastreabilidade, qualidade e performance otimizada para análises.")
        ]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("2.1 Camada Bronze (Raw Layer)")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [
          new TextRun({ text: "Objetivo: ", bold: true }),
          new TextRun("Armazenar dados brutos exatamente como vieram das fontes, sem qualquer transformação.")
        ]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun({ text: "Características:", bold: true })
        ]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Todos os campos armazenados como VARCHAR ou TEXT")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Payloads JSON completos salvos em campo JSONB para auditoria")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Metadados obrigatórios: data_carga_bronze, nome_arquivo_origem")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Nenhuma validação ou limpeza aplicada")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Schema: bronze.*")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun({ text: "Total: 16 tabelas", bold: true })]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_3,
        children: [new TextRun("2.1.1 Tabelas da Camada Bronze")]
      }),

      new Table({
        columnWidths: [4680, 4680],
        margins: { top: 100, bottom: 100, left: 180, right: 180 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                shading: { fill: "CD7F32", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Tabela", bold: true, size: 22, color: "FFFFFF" })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                shading: { fill: "CD7F32", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Fonte", bold: true, size: 22, color: "FFFFFF" })]
                })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.onedrive_clientes")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive/Clientes.csv")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.onedrive_contratos")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive/Contratos.csv")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.onedrive_produtos")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive/Produtos.csv")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.onedrive_precificacao")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive/Precificacao.csv")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.ploomes_deals")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Ploomes /Deals")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.ploomes_contacts")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Ploomes /Contacts")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.ploomes_organizations")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Ploomes /Organizations")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.omie_notas_fiscais")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Omie /NotasFiscais")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.omie_contas_receber")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Omie /ContasReceber")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.omie_contas_pagar")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Omie /ContasPagar")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.movidesk_tickets")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Movidesk /Tickets")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.finqi_consumo")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Finqi /Consumo")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.salesbox_consumo")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Salesbox /Uso")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.acertpix_transacoes")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API Acertpix /Transacoes")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("bronze.spc_consultas")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 4680, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("API SPC Brasil /Consultas")] })]
              })
            ]
          })
        ]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // Continuação - Camada Silver
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("2.2 Camada Silver (Curated Layer)")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [
          new TextRun({ text: "Objetivo: ", bold: true }),
          new TextRun("Transformar dados brutos em dados limpos, validados e prontos para análise.")
        ]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun({ text: "Transformações Aplicadas:", bold: true })
        ]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Conversão de tipos de dados (VARCHAR → DATE, NUMERIC, BOOLEAN)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Validação de CPF/CNPJ, emails, telefones")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Deduplicação usando ROW_NUMBER() OVER (PARTITION BY)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Padronização de categorias, status e nomenclaturas")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Criação de chaves primárias (PKs) e estrangeiras (FKs)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Merge de dados de múltiplas fontes (ex: clientes do OneDrive + Ploomes)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Tratamento de valores nulos e outliers")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Schema: credits.*")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun({ text: "Total: 5 tabelas principais", bold: true })]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_3,
        children: [new TextRun("2.2.1 Tabelas da Camada Silver")]
      }),

      new Table({
        columnWidths: [3120, 6240],
        margins: { top: 100, bottom: 100, left: 180, right: 180 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                shading: { fill: "C0C0C0", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Tabela", bold: true, size: 22 })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                shading: { fill: "C0C0C0", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Descrição", bold: true, size: 22 })]
                })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "credits.clientes", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Tabela central de clientes com merge de OneDrive + Ploomes. Dados validados (CPF/CNPJ, email) e deduplicados.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "credits.contratos", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Contratos com datas convertidas (DATE) e valores numéricos (NUMERIC). FK para credits.clientes.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "credits.produtos", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Catálogo de produtos com categorização e status padronizados.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "credits.notas_fiscais", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Notas fiscais do Omie com valores consolidados e FK para clientes.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "credits.consumo_parceiros", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("União de consumo de Finqi, Salesbox, Acertpix. Campo 'parceiro' identifica a fonte.")] })]
              })
            ]
          })
        ]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_3,
        children: [new TextRun("2.2.2 Relacionamentos Entre Tabelas")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [new TextRun("A camada Silver estabelece relacionamentos através de chaves primárias e estrangeiras:")]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("credits.clientes (cliente_pk) ← credits.contratos (cliente_pk)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("credits.clientes (cliente_pk) ← credits.notas_fiscais (cliente_pk)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("credits.clientes (cliente_pk) ← credits.consumo_parceiros (cliente_pk)")]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // Continuação - Camada Gold
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("2.3 Camada Gold (Analytics Layer)")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [
          new TextRun({ text: "Objetivo: ", bold: true }),
          new TextRun("Fornecer views e tabelas agregadas otimizadas para consumo direto em ferramentas de BI.")
        ]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun({ text: "Características:", bold: true })
        ]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Views materializadas ou convencionais conforme necessidade")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Agregações pré-calculadas (SUM, AVG, COUNT)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Métricas de negócio (KPIs, faturamento, pipeline)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Joins complexos simplificados")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Atualização periódica conforme frequência de uso")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Schema: credits.vw_*")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_3,
        children: [new TextRun("2.3.1 Views da Camada Gold")]
      }),

      new Table({
        columnWidths: [3120, 6240],
        margins: { top: 100, bottom: 100, left: 180, right: 180 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                shading: { fill: "FFD700", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "View", bold: true, size: 22 })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                shading: { fill: "FFD700", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Descrição e Métricas", bold: true, size: 22 })]
                })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_faturamento_mensal", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Receita consolidada por mês/cliente/produto. Métricas: faturamento total, ticket médio, quantidade de notas.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_consumo_mensal_parceiros", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Consolidação de consumo de serviços por parceiro (Finqi, Salesbox, etc). Métricas: quantidade consumida, valor total.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_pipeline_vendas", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Funil de vendas do Ploomes. Métricas: quantidade de deals por estágio, valor total, probabilidade média de ganho.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_performance_atendimento", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Análise de tickets Movidesk. Métricas: total de tickets, tickets fechados, tempo médio de resolução, satisfação.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_consumo_6_meses", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Histórico de consumo dos últimos 6 meses para análise de tendências.")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun({ text: "vw_metas_consultores", bold: true })] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 6240, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Acompanhamento de metas por consultor de vendas.")] })]
              })
            ]
          })
        ]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 3. FLUXO DE DADOS
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("3. Fluxo de Dados e Integração")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("O fluxo de dados segue o modelo ETL (Extract, Transform, Load) através das três camadas:")
        ]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("3.1 Ingestão Bronze (Extract)")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Extração: ", bold: true }), new TextRun("Scripts Python conectam às APIs ou leem CSVs do OneDrive")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Carga Raw: ", bold: true }), new TextRun("Dados inseridos nas tabelas bronze.* exatamente como recebidos")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Auditoria: ", bold: true }), new TextRun("Registro de data_carga_bronze e nome_arquivo_origem")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Frequência: ", bold: true }), new TextRun("Varia por fonte (1h para Movidesk, diária para OneDrive, etc.)")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("3.2 Transformação Silver (Transform)")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Limpeza: ", bold: true }), new TextRun("Remoção de duplicatas, tratamento de nulos")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Validação: ", bold: true }), new TextRun("Aplicação de regras de negócio e validações")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Conversão: ", bold: true }), new TextRun("Tipos de dados corretos (DATE, NUMERIC, BOOLEAN)")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Merge: ", bold: true }), new TextRun("Consolidação de múltiplas fontes (ex: clientes)")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Carga Silver: ", bold: true }), new TextRun("INSERT/UPDATE nas tabelas credits.* com controle de versão")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("3.3 Agregação Gold (Load)")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Agregação: ", bold: true }), new TextRun("Criação de views com GROUP BY, JOINs complexos")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Cálculos: ", bold: true }), new TextRun("KPIs, métricas de negócio pré-calculadas")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Otimização: ", bold: true }), new TextRun("Índices, views materializadas conforme necessidade")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun({ text: "Atualização: ", bold: true }), new TextRun("REFRESH periódico ou sob demanda")]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 4. ESTRUTURA DO REPOSITÓRIO
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("4. Estrutura do Repositório GitHub")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("O repositório está organizado para facilitar manutenção, deployment e escalabilidade:")
        ]
      }),

      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("dw-credits-brasil/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── sql/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── bronze/              # Scripts DDL camada Bronze")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── silver/              # Scripts DDL camada Silver")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── gold/                # Views camada Gold")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   └── init/                # Scripts de inicialização")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── python/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── ingestors/           # Scripts de ingestão por fonte")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── transformers/        # Transformações Bronze → Silver")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── utils/               # Funções utilitárias")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   └── schedulers/          # Orquestração e agendamento")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── docker/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── docker-compose.yml   # Configuração Docker completa")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   └── Dockerfile            # Imagem customizada se necessário")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── config/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── dev.env              # Variáveis ambiente dev")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── prod.env             # Variáveis ambiente prod")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   └── connections.yaml     # Configurações de conexão")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── docs/")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── api/                 # Documentação de APIs")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   ├── diagrams/            # Diagramas de arquitetura")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│   └── runbooks/            # Guias operacionais")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("│")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── tests/                   # Testes unitários e integração")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── .gitignore")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("├── README.md")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("└── requirements.txt         # Dependências Python")]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 5. INSTALAÇÃO E DEPLOYMENT
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("5. Instalação e Deployment")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("5.1 Ambiente Local (Desenvolvimento)")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [new TextRun({ text: "Pré-requisitos:", bold: true })]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Docker 20+ e Docker Compose")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Python 3.10+")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Git")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_3,
        children: [new TextRun("Passo a Passo")]
      }),

      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# 1. Clonar repositório")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("git clone https://github.com/credits-brasil/dw-credits-brasil.git")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("cd dw-credits-brasil")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# 2. Configurar variáveis de ambiente")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("cp config/dev.env.example config/dev.env")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# Editar dev.env com suas credenciais")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# 3. Subir containers Docker")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("cd docker")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("docker-compose up -d")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# 4. Executar scripts de inicialização")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("psql -U dw_admin -d credits_dw -f sql/init/01-create-schemas.sql")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("psql -U dw_admin -d credits_dw -f sql/bronze/create-all-tables.sql")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("psql -U dw_admin -d credits_dw -f sql/silver/create-all-tables.sql")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("# 5. Instalar dependências Python")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("python -m venv venv")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("source venv/bin/activate")]
      }),
      new Paragraph({
        style: "CodeBlock",
        children: [new TextRun("pip install -r requirements.txt")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("5.2 Deploy Azure (Produção)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("Após aprovação do orçamento, o deployment será realizado na Azure com a seguinte arquitetura:")
        ]
      }),

      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("VM Azure D8s_v5 (8 vCPUs, 32 GB RAM)")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Disco Premium SSD 1TB")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("PostgreSQL 15 em Docker")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Backup diário automatizado")]
      }),
      new Paragraph({
        numbering: { reference: "bullet-list", level: 0 },
        children: [new TextRun("Monitoramento com Azure Monitor")]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 6. AUTOMAÇÃO
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("6. Automação e Orquestração")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun("Os processos ETL são automatizados usando Python com agendamento via cron ou Apache Airflow.")
        ]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("6.1 Frequência de Atualização")]
      }),

      new Table({
        columnWidths: [3120, 3120, 3120],
        margins: { top: 100, bottom: 100, left: 180, right: 180 },
        rows: [
          new TableRow({
            tableHeader: true,
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                shading: { fill: "4472C4", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Fonte", bold: true, size: 22, color: "FFFFFF" })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                shading: { fill: "4472C4", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Frequência", bold: true, size: 22, color: "FFFFFF" })]
                })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                shading: { fill: "4472C4", type: ShadingType.CLEAR },
                children: [new Paragraph({
                  alignment: AlignmentType.CENTER,
                  children: [new TextRun({ text: "Horário", bold: true, size: 22, color: "FFFFFF" })]
                })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Movidesk")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("A cada 1 hora")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("7h às 19h")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Omie NF")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("A cada 2 horas")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("6h, 8h, 10h...")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Ploomes")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("A cada 4 horas")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("6h, 10h, 14h, 18h")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("OneDrive")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Diária")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("6h")] })]
              })
            ]
          }),
          new TableRow({
            children: [
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Finqi/Salesbox")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("Diária")] })]
              }),
              new TableCell({
                borders: cellBorders,
                width: { size: 3120, type: WidthType.DXA },
                children: [new Paragraph({ children: [new TextRun("7h")] })]
              })
            ]
          })
        ]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 7. PRÓXIMOS PASSOS
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("7. Próximos Passos")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("7.1 Fase 1: Desenvolvimento Local (Atual)")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Criar todas as tabelas Bronze, Silver e Gold")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Desenvolver scripts Python de ingestão")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Implementar transformações Bronze → Silver")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Criar views Gold para BI")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Testar localmente com dados de amostra")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("7.2 Fase 2: Deploy Azure (Pós-Aprovação)")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Provisionar VM Azure conforme especificações")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Configurar backups automatizados")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Migrar código e dados para produção")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Configurar monitoramento e alertas")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Integrar com Power BI")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("7.3 Fase 3: Otimização e Expansão")]
      }),

      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Implementar views materializadas para performance")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Adicionar novas fontes de dados conforme necessário")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Criar dashboards específicos por área")]
      }),
      new Paragraph({
        numbering: { reference: "numbered-list-1", level: 0 },
        children: [new TextRun("Implementar machine learning para previsões")]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // 8. CONTATOS E SUPORTE
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("8. Contatos e Suporte")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [
          new TextRun({ text: "Equipe de Projetos:", bold: true })
        ]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Email: projetos@creditsbrasil.com")]
      }),

      new Paragraph({
        spacing: { after: 60 },
        children: [
          new TextRun({ text: "Repositório GitHub:", bold: true })
        ]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("https://github.com/credits-brasil/dw-credits-brasil")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [
          new TextRun({ text: "Documentação Técnica Completa", bold: true, size: 22 }),
          new TextRun({ text: " | Credits Brasil © 2025", size: 20, color: "666666" })
        ]
      })
    ]
  }]
});

// Generate document
Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync('/mnt/user-data/outputs/DW_Credits_Brasil_Documentacao_Completa.docx', buffer);
  console.log('Documento criado com sucesso!');
});
