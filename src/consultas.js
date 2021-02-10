/*En esta consulta en primer lugar utilizamos el operador match para seleccionar
aquellos productos que han sido obtenidos por la distribuidora numero 1, a continuación
con los resultados mostrados hacemos un group con el cual mostraremos el mes de compra
, el producto y el calculo de los beneficios, todo ello aplicado a cada uno de los
documentos arrojados en el operador de etapa anterior (match), a continuación realizamos un
project con el fin de calcular el porcentaje de los beneficios que ira destinado
a los sueldos del trabajador y en último lugar procedemos a ordenar los resultados obtenidos
en el ultimo operador de etapa en función de los beneficios y de forma descendente, es decir
primero obtendremos aquellos productos en los que los beneficios hayan sido mayores.  */

db.almacen.aggregate(
    [
        {
            $match: {
                Distribuidora: 1
            }
        },
        {
            $group:
            {
                _id: {
                    Mes: { $month: "$fecha" },
                    producto: "$producto"
                },
                Beneficios: { $sum: { $multiply: ["$precioVENT", "$unidades"] } }
            }
        },
        {
            $project: {
                Beneficios: "$Beneficios",
                ParaSueldos: { $multiply: ["$Beneficios", 0.15] },
            }
        },
        {
            $sort: {
                Beneficios: -1
            }
        }]).pretty()


/*En esta consulta en primer lugar procedemos a realizar un match de aquellos documentos
cuya año de compra haya sido el 2020, a continuación agrupamos los documentos resultantes
en función de la distribuidora que los envío, ademas aprovechamos el group para realizar el calculo 
de los gastos de los productos que se han comprado en la distribuidora además de calcular la cantidad
de líquido que se ha obtenido vendiendo dichos productos. En el siguiente operador de etapa, un project,
procedemos a calcular la diferencia entre las ventas y los gastos lo cual nos da como resultado un beneficio neto,
con los resultados arrojados procedemos a realizar un segundo match en el cual filtramos para quedarnos unicamente con aquellos 
documentos en los que la diferencia entre ventas y gastos haya sido superior a 100. En último lugar 
ordenamos los resultados de forma descendente en cuanto a los gastos en la distribuidora.*/

db.almacen.aggregate(
    [
        {
            $match:
            {
                $expr: { $eq: [{ $year: "$fecha" }, 2020] }
            }
        },
        {
            $group:
            {
                _id: {
                    IdentificadorDistribuidora: "$Distribuidora"
                },
                GastosEnLaDistribuidora: { $sum: { $multiply: ["$precioCompra", "$unidades"] } },
                LiquidoEnVentas: { $sum: { $multiply: ["$precioVENT", "$unidades"] } }
            }
        },
        {
            $project: {
                LiquidoEnVentas: "$LiquidoEnVentas",
                GastosEnLaDistribuidora: "$GastosEnLaDistribuidora",
                Diferecia_Ventas_Gastos: { $subtract: ["$LiquidoEnVentas", "$GastosEnLaDistribuidora"] },
                total_prize: 1,
            }
        },
        {
            $match:
            {
                $expr: { $gt: ["$Diferecia_Ventas_Gastos", 100] }
            }
        },
        {
            $sort: {
                GastosEnLaDistribuidora: -1
            }
        }]).pretty()

/*      CONSULTA SIMPLE     
En esta consulta, de caracter más simple que las anteriores procedemos a separar aquellos productos
que fueron enviados por la distribuidora numero 2, tras esto calculamos mediante un project el margen de
beneficio que existe en una única unidad de estos productos para ello restamos al precio de venta de unidad
al precio que fue adquirida la unidad y en último lugar realizamos un match de aquellos documentos 
que presenten un margen de beneficio igual o superior a 1, para terminar ordenamos de forma ascendente*/

db.almacen.aggregate([
    {
        $match: {
            Distribuidora: 2
        }
    },
    {
        $project: {
            _id: 0,
            Producto: "$producto",
            BeneficioPorUnidad: { $round: { $subtract: ["$precioVENT", "$precioCompra"] } },
        }
    },
    {
        $match:
        {
            $expr: { $gte: ["$BeneficioPorUnidad", 1] }
        }
    },
    {
        $sort: {
            BeneficioPorUnidad: 1
        }
    }
]
