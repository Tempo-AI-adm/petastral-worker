const payload = {
  payment_id: "08241630-ce79-4177-a1e5-daad4ae63734",
  email: "gkaramvono@gmail.com",
  pet_data: {
    nome: "dada",
    tipo: "dog",
    raca: "Corgi",
    porte: "pequeno",
    pelo: "curto",
    cor: ["caramelo", "branco"],
    sexo: "femea",
    mes: 4,
    dia: 10,
    cidade: "São Paulo",
    signo_pet: "Áries",
    elemento: "fogo",
    score: 72,
    email: "gkaramvono@gmail.com"
  }
}

fetch('https://petastral-worker.onrender.com/generate', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload)
})
.then(r => r.json())
.then(console.log)
.catch(console.error)
