"""Validacion de las solicitudes y sus parametros."""
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class SolicitudBase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timeout: float = Field(default=30, ge=0.01, le=3600, allow_inf_nan=False)


class Esperar(SolicitudBase):
    tipo: Literal["esperar"]
    segundos: float = Field(ge=0, le=3600, allow_inf_nan=False)


class Hashear(SolicitudBase):
    tipo: Literal["hashear"]
    texto: str = Field(max_length=100_000)
    iteraciones: int = Field(ge=1, le=10_000_000, strict=True)


class Descargar(SolicitudBase):
    tipo: Literal["descargar"]
    urls: list[HttpUrl] = Field(min_length=1, max_length=100)


Solicitud = Annotated[Esperar | Hashear | Descargar, Field(discriminator="tipo")]
Estado = Literal["pendiente", "ejecutando", "completada", "fallida", "cancelada"]
